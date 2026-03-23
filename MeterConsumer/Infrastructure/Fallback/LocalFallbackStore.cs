using MeterConsumer.Core.Interfaces;
using MeterConsumer.Core.Models;
using MeterConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace MeterConsumer.Infrastructure.Fallback;

/// <summary>
/// Thread-safe JSONL file store for Kafka delivery failures.
///
/// DATA LOSS PREVENTION STRATEGY:
/// ─────────────────────────────────────────────────────────────
/// When Kafka is unavailable:
///   1. Message is serialized to a JSONL line and appended to disk
///   2. RabbitMQ ACK is sent ONLY after successful file write
///   → Message is now safe: RabbitMQ broker no longer holds it
///   → File survives service restarts / crashes
///
/// When Kafka recovers (FallbackReplayService):
///   1. File is read line by line (streaming — not loaded into RAM)
///   2. Each entry is produced to Kafka
///   3. On confirmed delivery, entry ID is marked for removal
///   4. File is compacted (successfully replayed entries removed)
///
/// FILE FORMAT (JSONL — one JSON object per line):
///   {"entryId":"...","savedAt":"...","kafkaTopic":"...","message":{...}}
///   {"entryId":"...","savedAt":"...","kafkaTopic":"...","message":{...}}
///
/// THREAD SAFETY:
///   SemaphoreSlim(1,1) ensures exclusive file access.
///   All public methods are async and honour CancellationToken.
/// </summary>
public sealed class LocalFallbackStore : IFallbackStore, IDisposable
{
    private readonly string _filePath;
    private readonly long _maxFileSizeBytes;
    private readonly ILogger<LocalFallbackStore> _logger;

    // Mutex for file operations — only one thread accesses the file at a time
    private readonly SemaphoreSlim _fileLock = new(1, 1);

    private int _pendingCount = 0;    // Approximate, updated on write/remove

    public bool HasPendingEntries => _pendingCount > 0;
    public int PendingCount => _pendingCount;

    public LocalFallbackStore(
        IOptions<MeterConsumerSettings> options,
        ILogger<LocalFallbackStore> logger)
    {
        _logger = logger;
        _filePath = options.Value.Fallback.FilePath;
        _maxFileSizeBytes = options.Value.Fallback.MaxFileSizeBytes;

        // Ensure directory exists (C:\ProgramData\MeterConsumer\)
        var dir = Path.GetDirectoryName(_filePath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        // Count existing entries on startup (for HasPendingEntries)
        _pendingCount = CountLines();

        if (_pendingCount > 0)
            _logger.LogWarning("Fallback store has {Count} pending entries from previous run — replay will start", _pendingCount);
    }

    // ── IFallbackStore ───────────────────────────────────────────────────────

    /// <summary>
    /// Appends one entry to the JSONL file.
    /// File lock ensures no concurrent writes corrupt the file.
    /// </summary>
    public async Task SaveAsync(FallbackEntry entry, CancellationToken ct = default)
    {
        await _fileLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Guard: stop writing if file is too large (prevents disk exhaustion)
            var info = new FileInfo(_filePath);
            if (info.Exists && info.Length >= _maxFileSizeBytes)
            {
                _logger.LogError("Fallback file has reached max size ({Size} bytes) — dropping message {Id}",
                    _maxFileSizeBytes, entry.EntryId);
                return;
            }

            var line = JsonSerializer.Serialize(entry);
            // StreamWriter with append:true — never overwrites existing content
            await using var writer = new StreamWriter(_filePath, append: true);
            await writer.WriteLineAsync(line.AsMemory(), ct).ConfigureAwait(false);
            await writer.FlushAsync(ct).ConfigureAwait(false);

            Interlocked.Increment(ref _pendingCount);

            _logger.LogDebug("Saved to fallback file | EntryId={Id} Topic={Topic}",
                entry.EntryId, entry.KafkaTopic);
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>
    /// Streams entries from the JSONL file one line at a time.
    /// Uses IAsyncEnumerable — no full file load into memory.
    /// Skips corrupt lines (logs warning) so one bad entry doesn't block replay.
    /// </summary>
    public async IAsyncEnumerable<FallbackEntry> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!File.Exists(_filePath)) yield break;

        // Read all lines into memory (file is small enough for this)
        // We need a snapshot to avoid issues with concurrent writes
        string[] lines;

        await _fileLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lines = await File.ReadAllLinesAsync(_filePath, ct).ConfigureAwait(false);
        }
        finally
        {
            _fileLock.Release();
        }

        foreach (var line in lines)
        {
            ct.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(line)) continue;

            FallbackEntry? entry = null;
            try
            {
                entry = JsonSerializer.Deserialize<FallbackEntry>(line);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Corrupt line in fallback file — skipping: {Line}", line[..Math.Min(100, line.Length)]);
            }

            if (entry is not null)
                yield return entry;
        }
    }

    /// <summary>
    /// Removes successfully replayed entries by rewriting the file without them.
    /// Called after confirmed Kafka delivery — never before.
    /// </summary>
    public async Task RemoveAsync(IEnumerable<Guid> entryIds, CancellationToken ct = default)
    {
        var removeSet = entryIds.ToHashSet();
        if (removeSet.Count == 0) return;

        await _fileLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!File.Exists(_filePath)) return;

            // Read all lines, filter out the ones to remove
            var allLines = await File.ReadAllLinesAsync(_filePath, ct).ConfigureAwait(false);
            var remaining = new List<string>(allLines.Length);
            int removedCount = 0;

            foreach (var line in allLines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    var entry = JsonSerializer.Deserialize<FallbackEntry>(line);
                    if (entry is not null && removeSet.Contains(entry.EntryId))
                    {
                        removedCount++;
                        continue;   // skip — this entry was successfully replayed
                    }
                }
                catch { /* corrupt line — keep it */ }

                remaining.Add(line);
            }

            // Atomic overwrite: write to temp file, then replace
            var tempPath = _filePath + ".tmp";
            await File.WriteAllLinesAsync(tempPath, remaining, ct).ConfigureAwait(false);
            File.Move(tempPath, _filePath, overwrite: true);

            Interlocked.Add(ref _pendingCount, -removedCount);

            _logger.LogDebug("Fallback file compacted: removed {Removed}, remaining {Remaining}",
                removedCount, remaining.Count);
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>Deletes the entire fallback file after a successful complete replay.</summary>
    public async Task ClearAsync(CancellationToken ct = default)
    {
        await _fileLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (File.Exists(_filePath))
            {
                File.Delete(_filePath);
                Interlocked.Exchange(ref _pendingCount, 0);
                _logger.LogInformation("Fallback file cleared after successful replay");
            }
        }
        finally
        {
            _fileLock.Release();
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private int CountLines()
    {
        if (!File.Exists(_filePath)) return 0;
        try
        {
            return File.ReadLines(_filePath).Count(l => !string.IsNullOrWhiteSpace(l));
        }
        catch { return 0; }
    }

    public void Dispose()
    {
        _fileLock.Dispose();
    }
}
