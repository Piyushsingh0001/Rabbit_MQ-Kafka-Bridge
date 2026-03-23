using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using KafkaConsumer.Core.Events;
using KafkaConsumer.Core.Interfaces;
using KafkaConsumer.Core.Models;
using KafkaConsumer.Infrastructure.Configuration;
using KafkaConsumer.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Infrastructure.FileWriter;

/// <summary>
/// Reads ConsumedMessages from the pipeline Channel and writes them
/// as JSON lines to daily per-topic text files.
///
/// OUTPUT FILE FORMAT:
///   C:\ProgramData\KafkaConsumer\Output\meter_voltage_2026-03-19.txt
///   C:\ProgramData\KafkaConsumer\Output\meter_current_2026-03-19.txt
///
///   Each line = raw JSON exactly as received from Kafka:
///   {"meterId":105,"voltage":221.5,"timestamp":"2026-03-19T07:04:42Z"}
///   {"meterId":106,"voltage":218.0,"timestamp":"2026-03-19T07:04:43Z"}
///
/// KEY DESIGN:
///  - ZERO Kafka dependency — no IConsumer, no offset commits here
///  - Kafka auto-commits offsets independently in KafkaConsumerService
///  - Batched writes: accumulate BatchSize messages OR FlushIntervalMs — whichever first
///  - File write failure: messages go to ConcurrentQueue retry queue
///    Offset NOT committed until file write succeeds (via Kafka reconnect re-delivery)
///  - Per-topic StreamWriter pool: one open writer per topic-per-day
///  - Midnight rollover: old writer closed, new daily file opened automatically
///  - RetryWorkerLoop: concurrent loop drains retry queue independently
/// </summary>
public sealed class FileWriterService : IFileWriterService
{
    private readonly ILogger<FileWriterService> _logger;
    private readonly FileWriterSettings _settings;
    private readonly MessagePipeline _pipeline;

    // One open StreamWriter per "topicName_yyyy-MM-dd" key
    // Kept open across batches — avoids expensive open/close on every write
    private readonly Dictionary<string, StreamWriter> _writerPool = new();
    private readonly SemaphoreSlim _writerPoolLock = new(1, 1);

    // Bounded retry queue — messages that failed file write
    private readonly ConcurrentQueue<ConsumedMessage> _retryQueue = new();
    private int _retryQueueCount = 0;   // Interlocked — lock-free count

    public event EventHandler<BatchWrittenEventArgs>?       OnBatchWritten;
    public event EventHandler<FileWriteFailedEventArgs>?    OnWriteFailed;
    public event EventHandler<MessageRetryQueuedEventArgs>? OnMessageRetryQueued;

    public int RetryQueueDepth => _retryQueueCount;

    private bool _disposed;

    public FileWriterService(
        IOptions<KafkaConsumerSettings> options,
        MessagePipeline pipeline,
        ILogger<FileWriterService> logger)
    {
        _settings = options.Value.FileWriter;
        _pipeline = pipeline;
        _logger   = logger;

        // Create output directory at startup — fail fast if path is wrong
        Directory.CreateDirectory(_settings.OutputDirectory);
        _logger.LogInformation("Output directory ready: {Dir}", _settings.OutputDirectory);
    }

    // ── Entry Point ───────────────────────────────────────────────────────────

    public async Task RunAsync(CancellationToken ct)
    {
        _logger.LogInformation("FileWriterService started");

        // Main write loop + retry loop run concurrently
        await Task.WhenAll(
            MainWriteLoopAsync(ct),
            RetryWorkerLoopAsync(ct)
        ).ConfigureAwait(false);

        // Flush and close all open StreamWriters on exit
        await CloseAllWritersAsync().ConfigureAwait(false);

        _logger.LogInformation("FileWriterService stopped — all writers closed");
    }

    // ── Main Write Loop ───────────────────────────────────────────────────────

    private async Task MainWriteLoopAsync(CancellationToken ct)
    {
        var batch = new List<ConsumedMessage>(_settings.BatchSize);

        while (!ct.IsCancellationRequested || _pipeline.Reader.CanPeek)
        {
            batch.Clear();
            var deadline = DateTime.UtcNow.AddMilliseconds(_settings.FlushIntervalMs);

            // Accumulate messages until BatchSize reached OR flush interval elapses
            while (batch.Count < _settings.BatchSize)
            {
                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero) break;

                // Create a linked token that fires after 'remaining' ms
                // This lets ReadAsync wake up at the flush deadline
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                linkedCts.CancelAfter(remaining);

                try
                {
                    var msg = await _pipeline.Reader
                        .ReadAsync(linkedCts.Token)
                        .ConfigureAwait(false);

                    batch.Add(msg);
                }
                catch (OperationCanceledException)
                {
                    // Flush interval elapsed OR service stopping — flush what we have
                    break;
                }
                catch (ChannelClosedException)
                {
                    // Pipeline marked Complete() by KafkaConsumerService — drain remaining
                    break;
                }
            }

            if (batch.Count == 0) continue;

            // Group by topic name — each group goes to a different daily file
            var byTopic = batch.GroupBy(m => m.TopicName);
            foreach (var topicGroup in byTopic)
                await WriteBatchAsync(topicGroup.ToList(), ct).ConfigureAwait(false);
        }

        // Final drain — get anything left in the Channel after cancellation
        await DrainRemainingAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Non-blocking drain of anything left in the Channel after shutdown.</summary>
    private async Task DrainRemainingAsync(CancellationToken ct)
    {
        var remaining = new List<ConsumedMessage>();
        while (_pipeline.Reader.TryRead(out var msg))
            remaining.Add(msg);

        if (remaining.Count == 0) return;

        _logger.LogInformation("Draining {Count} remaining messages", remaining.Count);

        foreach (var group in remaining.GroupBy(m => m.TopicName))
            await WriteBatchAsync(group.ToList(), ct).ConfigureAwait(false);
    }

    // ── Batch Write ───────────────────────────────────────────────────────────

    /// <summary>
    /// Writes one batch of same-topic messages to the daily file.
    ///
    /// SUCCESS → OnBatchWritten event raised.
    /// FAILURE (after MaxFileWriteRetries) → messages go to RetryQueue.
    ///
    /// IMPORTANT: File write and offset commit are completely separate.
    /// Offset commit happens automatically in KafkaConsumerService.
    /// This method ONLY handles file I/O.
    /// </summary>
    private async Task WriteBatchAsync(List<ConsumedMessage> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        var topic    = batch[0].TopicName;
        var dateKey  = batch[0].DateKey;
        var filePath = BuildFilePath(topic, dateKey);
        int attempt  = 0;

        while (attempt <= _settings.MaxFileWriteRetries)
        {
            try
            {
                // Write all messages in this batch to the file
                await WriteLinesToFileAsync(filePath, batch, ct).ConfigureAwait(false);

                // ── SUCCESS ───────────────────────────────────────────────────
                _logger.LogInformation(
                    "[WRITTEN] {Topic} | {Count} messages | Offsets {F}–{L} | {File}",
                    topic, batch.Count,
                    batch.Min(m => m.Offset),
                    batch.Max(m => m.Offset),
                    Path.GetFileName(filePath));

                OnBatchWritten?.Invoke(this, new BatchWrittenEventArgs
                {
                    TopicName    = topic,
                    FilePath     = filePath,
                    MessageCount = batch.Count,
                    FirstOffset  = batch.Min(m => m.Offset),
                    LastOffset   = batch.Max(m => m.Offset)
                });

                return; // Done — exit retry loop
            }
            catch (Exception ex)
            {
                attempt++;
                bool willRetry = attempt <= _settings.MaxFileWriteRetries;

                OnWriteFailed?.Invoke(this, new FileWriteFailedEventArgs
                {
                    TopicName    = topic,
                    FilePath     = filePath,
                    MessageCount = batch.Count,
                    RetryAttempt = attempt,
                    Exception    = ex,
                    WillRetry    = willRetry
                });

                if (!willRetry)
                {
                    // All retries exhausted — send to retry queue for later attempt
                    _logger.LogError(
                        "[WRITE FAILED] Max retries reached for {Topic} — sending {Count} messages to retry queue",
                        topic, batch.Count);
                    EnqueueForRetry(batch);
                    return;
                }

                _logger.LogWarning(ex,
                    "[WRITE FAILED] {Topic} attempt {A}/{Max} — retrying",
                    topic, attempt, _settings.MaxFileWriteRetries);

                // Exponential backoff between retries: 2s, 4s, 8s...
                var delayMs = Math.Min(
                    _settings.FileWriteRetryDelayMs * Math.Pow(2, attempt - 1),
                    30_000);
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
            }
        }
    }

    // ── File I/O ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Core file write: appends one JSON line per message to the daily file.
    /// Uses a pooled StreamWriter — stays open across batches for performance.
    /// SemaphoreSlim ensures only one thread writes at a time (no corruption).
    /// </summary>
    private async Task WriteLinesToFileAsync(
        string filePath,
        List<ConsumedMessage> batch,
        CancellationToken ct)
    {
        await _writerPoolLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var writer = await GetOrCreateWriterAsync(filePath).ConfigureAwait(false);

            // Build entire batch as one string — single write call is faster
            var sb = new StringBuilder(batch.Count * 120);
            foreach (var msg in batch)
            {
                // Write the raw JSON string exactly as received from Kafka
                // e.g.: {"meterId":105,"voltage":221.5,"timestamp":"2026-03-19T07:04:42Z"}
                sb.AppendLine(msg.RawJson);
            }

            await writer.WriteAsync(sb, ct).ConfigureAwait(false);

            // Flush after every batch — ensures data is on disk, not just in OS buffer
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _writerPoolLock.Release();
        }
    }

    // ── StreamWriter Pool ─────────────────────────────────────────────────────

    /// <summary>
    /// Returns the open StreamWriter for the given file path.
    /// Creates a new writer if:
    ///   a) This path has never been opened, OR
    ///   b) The date rolled over (midnight) — old writer is closed, new file created
    /// </summary>
    private async Task<StreamWriter> GetOrCreateWriterAsync(string filePath)
    {
        // Fast path — writer already open for this exact file path (today's file)
        if (_writerPool.TryGetValue(filePath, out var existing))
            return existing;

        // Find and close any stale writers for this topic (from a previous day)
        // Key prefix = "C:\...\meter_voltage_" (before the date part)
        var keyPrefix = filePath[..filePath.LastIndexOf('_')];
        var staleKeys = _writerPool.Keys
            .Where(k => k.StartsWith(keyPrefix, StringComparison.OrdinalIgnoreCase))
            .ToList();

        foreach (var staleKey in staleKeys)
        {
            if (_writerPool.Remove(staleKey, out var stale))
            {
                await stale.FlushAsync().ConfigureAwait(false);
                await stale.DisposeAsync().ConfigureAwait(false);
                _logger.LogInformation("Date rollover — closed: {Key}", Path.GetFileName(staleKey));
            }
        }

        // Open new StreamWriter in append mode
        // append:true = safe on service restart — does not overwrite existing data
        var newWriter = new StreamWriter(filePath, append: true, Encoding.UTF8)
        {
            AutoFlush = false   // Manual flush after each batch (more efficient)
        };

        _writerPool[filePath] = newWriter;
        _logger.LogInformation("Opened file: {Path}", filePath);
        return newWriter;
    }

    private string BuildFilePath(string topic, string dateKey)
        => Path.Combine(_settings.OutputDirectory, $"{topic}_{dateKey}.txt");

    // ── Retry Queue ───────────────────────────────────────────────────────────

    private void EnqueueForRetry(List<ConsumedMessage> batch)
    {
        foreach (var msg in batch)
        {
            if (_retryQueueCount >= _settings.RetryQueueCapacity)
            {
                _logger.LogCritical(
                    "Retry queue FULL ({Cap}) — dropping Seq={Seq} Topic={Topic} Offset={Off}",
                    _settings.RetryQueueCapacity, msg.SequenceId, msg.TopicName, msg.Offset);
                continue;
            }

            _retryQueue.Enqueue(msg);
            Interlocked.Increment(ref _retryQueueCount);

            OnMessageRetryQueued?.Invoke(this, new MessageRetryQueuedEventArgs
            {
                SequenceId     = msg.SequenceId,
                TopicName      = msg.TopicName,
                RetryCount     = msg.RetryCount,
                RetryQueueDepth = _retryQueueCount
            });
        }
    }

    // ── Retry Worker Loop ─────────────────────────────────────────────────────

    /// <summary>
    /// Runs concurrently with the main write loop.
    /// Wakes up every FlushIntervalMs, drains the retry queue, retries file writes.
    /// </summary>
    private async Task RetryWorkerLoopAsync(CancellationToken ct)
    {
        _logger.LogInformation("Retry worker started");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_settings.FlushIntervalMs), ct)
                    .ConfigureAwait(false);

                if (_retryQueue.IsEmpty) continue;

                await ProcessRetryQueueAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in retry worker — continuing");
            }
        }

        _logger.LogInformation("Retry worker stopped");
    }

    private async Task ProcessRetryQueueAsync(CancellationToken ct)
    {
        var snapshot = new List<ConsumedMessage>();
        
        while (_retryQueue.TryDequeue(out var msg))
        {
            Interlocked.Decrement(ref _retryQueueCount);
            msg.RetryCount++;
            snapshot.Add(msg);
        }

        if (snapshot.Count == 0) return;

        _logger.LogInformation("Retrying {Count} messages from retry queue", snapshot.Count);

        foreach (var group in snapshot.GroupBy(m => (m.TopicName, m.DateKey)))
        {
            var batch    = group.ToList();
            var filePath = BuildFilePath(group.Key.TopicName, group.Key.DateKey);

            try
            {
                await WriteLinesToFileAsync(filePath, batch, ct).ConfigureAwait(false);

                _logger.LogInformation(
                    "Retry SUCCESS — wrote {Count} messages to {File}",
                    batch.Count, Path.GetFileName(filePath));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retry FAILED for {Topic}", group.Key.TopicName);

                var stillRetriable = batch
                    .Where(m => m.RetryCount < _settings.MaxFileWriteRetries)
                    .ToList();

                var exhausted = batch
                    .Where(m => m.RetryCount >= _settings.MaxFileWriteRetries)
                    .ToList();

                foreach (var msg in exhausted)
                    _logger.LogCritical(
                        "PERMANENTLY DROPPED: Seq={Seq} Topic={Topic} Offset={Off}",
                        msg.SequenceId, msg.TopicName, msg.Offset);

                if (stillRetriable.Count > 0)
                    EnqueueForRetry(stillRetriable);
            }
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    private async Task CloseAllWritersAsync()
    {
        await _writerPoolLock.WaitAsync().ConfigureAwait(false);
        try
        {
            foreach (var (key, writer) in _writerPool)
            {
                try
                {
                    await writer.FlushAsync().ConfigureAwait(false);
                    await writer.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error closing writer {Key}", key);
                }
            }
            _writerPool.Clear();
        }
        finally
        {
            _writerPoolLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await CloseAllWritersAsync().ConfigureAwait(false);
        _writerPoolLock.Dispose();

        _logger.LogInformation("FileWriterService disposed");
    }
}
