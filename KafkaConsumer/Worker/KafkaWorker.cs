using KafkaConsumer.Core.Events;
using KafkaConsumer.Core.Interfaces;
using KafkaConsumer.Infrastructure.Configuration;
using KafkaConsumer.Infrastructure.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Worker;

/// <summary>
/// Windows Service entry point.
///
/// RESPONSIBILITIES:
///  1. Wire all events (subscribe)
///  2. Start KafkaConsumerService and FileWriterService concurrently
///  3. Log health metrics every N seconds
///  4. Graceful shutdown: cancel → drain pipeline → dispose
///
/// CONCURRENCY MODEL:
///  KafkaConsumerService.RunAsync()  ─── Task A (polls Kafka, writes to Channel)
///  FileWriterService.RunAsync()     ─── Task B (reads from Channel, writes files)
///
///  Task.WhenAll(A, B) — both run simultaneously.
///  A finishes first (Kafka stopped) → pipeline.Complete() → B drains and exits.
///  B finishes first (unlikely) → A continues until cancellation.
/// </summary>
public sealed class KafkaWorker : BackgroundService
{
    private readonly ILogger<KafkaWorker> _logger;
    private readonly IKafkaConsumerService _kafkaConsumer;
    private readonly IFileWriterService _fileWriter;
    private readonly MessagePipeline _pipeline;
    private readonly int _healthIntervalSeconds;

    // ── Metrics (Interlocked — lock-free thread-safe increments) ─────────────
    private long _totalConsumed = 0;
    private long _totalWritten = 0;
    private long _totalFailed = 0;

    private Timer? _healthTimer;

    public KafkaWorker(
        IKafkaConsumerService kafkaConsumer,
        IFileWriterService fileWriter,
        MessagePipeline pipeline,
        IOptions<KafkaConsumerSettings> options,
        ILogger<KafkaWorker> logger)
    {
        _kafkaConsumer = kafkaConsumer;
        _fileWriter = fileWriter;
        _pipeline = pipeline;
        _healthIntervalSeconds = options.Value.Processing.HealthLogIntervalSeconds;
        _logger = logger;
    }

    // ── BackgroundService ─────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaConsumer service starting...");

        try
        {
            // Step 1: Subscribe to all events
            WireEvents();

            // Step 2: Start health logging timer
            _healthTimer = new Timer(
                LogHealth,
                state: null,
                dueTime: TimeSpan.FromSeconds(_healthIntervalSeconds),
                period: TimeSpan.FromSeconds(_healthIntervalSeconds));

            _logger.LogInformation(
                "KafkaConsumer service RUNNING — writing to {Dir}",
                "C:\\ProgramData\\KafkaConsumer\\Output");

            // Step 3: Run both services concurrently
            // Task.WhenAll: both run simultaneously; exits when both complete.
            // Kafka consumer completing (shutdown) → pipeline.Complete()
            // → FileWriter drains channel → FileWriter completes
            // → WhenAll resolves
            await Task.WhenAll(
                _fileWriter.RunAsync(stoppingToken),
                _kafkaConsumer.RunAsync(stoppingToken)
            ).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("KafkaConsumer service shutting down...");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "KafkaConsumer service encountered a fatal error");
            throw;
        }
        finally
        {
            await CleanupAsync().ConfigureAwait(false);
        }
    }

    // ── Event Wiring ──────────────────────────────────────────────────────────

    private void WireEvents()
    {
        // KafkaConsumerService events
        _kafkaConsumer.OnConnectionChanged   += OnKafkaConnectionChangedHandler;
        _kafkaConsumer.OnMessageConsumed     += OnMessageConsumedHandler;

        // FileWriterService events
        _fileWriter.OnBatchWritten           += OnBatchWrittenHandler;
        _fileWriter.OnWriteFailed            += OnWriteFailedHandler;
        _fileWriter.OnMessageRetryQueued     += OnMessageRetryQueuedHandler;

        _logger.LogDebug("All events wired");
    }

    private void UnwireEvents()
    {
        _kafkaConsumer.OnConnectionChanged   -= OnKafkaConnectionChangedHandler;
        _kafkaConsumer.OnMessageConsumed     -= OnMessageConsumedHandler;
        _fileWriter.OnBatchWritten           -= OnBatchWrittenHandler;
        _fileWriter.OnWriteFailed            -= OnWriteFailedHandler;
        _fileWriter.OnMessageRetryQueued     -= OnMessageRetryQueuedHandler;

        _logger.LogDebug("All events unwired");
    }

    // ── Event Handlers ────────────────────────────────────────────────────────

    private void OnKafkaConnectionChangedHandler(object? sender, KafkaConnectionEventArgs e)
    {
        if (e.IsConnected)
            _logger.LogInformation("[KAFKA] CONNECTED: {Reason}", e.Reason);
        else
            _logger.LogWarning("[KAFKA] DISCONNECTED (attempt {A}): {Reason}",
                e.ReconnectAttempt, e.Reason);
    }

    private void OnMessageConsumedHandler(object? sender, MessageConsumedEventArgs e)
    {
        Interlocked.Increment(ref _totalConsumed);
        _logger.LogDebug(
            "[CONSUMED] Seq={Seq} Topic={Topic} Part={P} Off={O} MeterId={M}",
            e.SequenceId, e.TopicName, e.Partition, e.Offset, e.MeterId);
    }

    private void OnBatchWrittenHandler(object? sender, BatchWrittenEventArgs e)
    {
        Interlocked.Add(ref _totalWritten, e.MessageCount);
        _logger.LogInformation(
            "[WRITTEN] Topic={Topic} Count={N} Offsets={F}-{L} File={File}",
            e.TopicName, e.MessageCount, e.FirstOffset, e.LastOffset,
            Path.GetFileName(e.FilePath));
    }

    private void OnWriteFailedHandler(object? sender, FileWriteFailedEventArgs e)
    {
        Interlocked.Increment(ref _totalFailed);

        if (e.WillRetry)
            _logger.LogWarning(
                "[WRITE FAILED] Topic={Topic} Count={N} Attempt={A} — will retry. Error: {Err}",
                e.TopicName, e.MessageCount, e.RetryAttempt, e.Exception?.Message);
        else
            _logger.LogError(
                "[WRITE FAILED - MAX RETRIES] Topic={Topic} Count={N} — moving to retry queue. Error: {Err}",
                e.TopicName, e.MessageCount, e.Exception?.Message);
    }

    private void OnMessageRetryQueuedHandler(object? sender, MessageRetryQueuedEventArgs e)
    {
        _logger.LogWarning(
            "[RETRY QUEUED] Seq={Seq} Topic={Topic} RetryCount={R} QueueDepth={D}",
            e.SequenceId, e.TopicName, e.RetryCount, e.RetryQueueDepth);
    }

    // ── Health Logging ────────────────────────────────────────────────────────

    private void LogHealth(object? state)
    {
        var consumed  = Interlocked.Read(ref _totalConsumed);
        var written   = Interlocked.Read(ref _totalWritten);
        var failed    = Interlocked.Read(ref _totalFailed);

        _logger.LogInformation(
            "[HEALTH] Consumed={C} Written={W} Failed={F} " +
            "RetryQueue={R} ChannelDepth={Ch} KafkaUp={K}",
            consumed, written, failed,
            _fileWriter.RetryQueueDepth,
            _pipeline.Count,
            _kafkaConsumer.IsConnected);
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    private async Task CleanupAsync()
    {
        _logger.LogInformation("KafkaConsumer cleanup starting...");

        UnwireEvents();

        _healthTimer?.Dispose();

        await _kafkaConsumer.DisposeAsync().ConfigureAwait(false);
        await _fileWriter.DisposeAsync().ConfigureAwait(false);
        _pipeline.Dispose();

        _logger.LogInformation("KafkaConsumer cleanup complete");
    }
}
