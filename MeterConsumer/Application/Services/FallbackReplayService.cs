using MeterConsumer.Application.Services;
using MeterConsumer.Core.Events;
using MeterConsumer.Core.Interfaces;
using MeterConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MeterConsumer.Application.Services;

/// <summary>
/// Background loop that replays the fallback JSONL file to Kafka when Kafka recovers.
///
/// REPLAY WORKFLOW:
/// ─────────────────────────────────────────────────────────────
///
///  Every ReplayIntervalSeconds:
///     1. Check: fallback file has entries AND Kafka is available
///     2. Signal MessageDispatchService: "replay starting — buffer live messages"
///     3. Stream entries from file (ReplayBatchSize at a time)
///     4. For each entry: ProduceAsync to Kafka
///        - Success → collect entry ID for removal
///        - Failure → circuit will open → abort replay, retry next cycle
///     5. Remove successful entries from file
///     6. If file is empty → clear file entirely
///     7. Signal MessageDispatchService: "replay done — drain live buffer"
///     8. Drain live buffer (in order)
///
/// ORDERING GUARANTEE:
///   File entries are written in arrival order.
///   Replay reads them in the same order.
///   Live messages are buffered DURING replay, then processed AFTER.
///   Result: file messages are always processed before live messages.
///
/// BACKPRESSURE:
///   If Kafka fails mid-replay, circuit opens, replay stops.
///   Live messages continue accumulating in file / buffer.
///   Replay resumes next cycle when circuit half-opens.
/// ─────────────────────────────────────────────────────────────
/// </summary>
public sealed class FallbackReplayService : IAsyncDisposable
{
    private readonly ILogger<FallbackReplayService> _logger;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly IFallbackStore _fallbackStore;
    private readonly MessageDispatchService _dispatcher;
    private readonly FallbackSettings _settings;

    public event EventHandler<FallbackReplayEventArgs>? OnReplayStateChanged;

    public FallbackReplayService(
        IKafkaProducer kafkaProducer,
        IFallbackStore fallbackStore,
        MessageDispatchService dispatcher,
        IOptions<MeterConsumerSettings> options,
        ILogger<FallbackReplayService> logger)
    {
        _kafkaProducer = kafkaProducer;
        _fallbackStore = fallbackStore;
        _dispatcher = dispatcher;
        _settings = options.Value.Fallback;
        _logger = logger;
    }

    /// <summary>
    /// Main loop — runs on a background Task until cancellation.
    /// Wakes up every ReplayIntervalSeconds to check and replay.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        _logger.LogInformation("FallbackReplayService started — checking every {Interval}s",
            _settings.ReplayIntervalSeconds);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Wait before each check (also handles first-run delay on startup)
                await Task.Delay(TimeSpan.FromSeconds(_settings.ReplayIntervalSeconds), ct)
                    .ConfigureAwait(false);

                // Only replay if: file has entries AND Kafka is accepting messages
                if (_fallbackStore.HasPendingEntries && _kafkaProducer.IsAvailable)
                {
                    await ReplayCycleAsync(ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;  // Clean shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in replay loop — will retry next cycle");
                // Don't crash the loop — log and continue
            }
        }

        _logger.LogInformation("FallbackReplayService stopped");
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private async Task ReplayCycleAsync(CancellationToken ct)
    {
        _logger.LogInformation("Fallback replay starting — {Count} entries pending",
            _fallbackStore.PendingCount);

        // Step 1: Signal dispatcher to buffer incoming live messages
        _dispatcher.SetReplayingState(isReplaying: true);
        OnReplayStateChanged?.Invoke(this, new FallbackReplayEventArgs { IsStarting = true });

        int totalReplayed = 0;
        bool aborted = false;

        try
        {
            var successIds = new List<Guid>(_settings.ReplayBatchSize);

            await foreach (var entry in _fallbackStore.ReadAllAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                // If circuit opened mid-replay, stop and retry next cycle
                if (!_kafkaProducer.IsAvailable)
                {
                    _logger.LogWarning("Kafka circuit opened mid-replay — aborting. Will retry next cycle");
                    aborted = true;
                    break;
                }

                var delivered = await _kafkaProducer
                    .ProduceAsync(entry.Message, ct)
                    .ConfigureAwait(false);

                if (delivered)
                {
                    successIds.Add(entry.EntryId);
                    totalReplayed++;

                    // Compact file in batches — avoid holding large lists in memory
                    if (successIds.Count >= _settings.ReplayBatchSize)
                    {
                        await _fallbackStore.RemoveAsync(successIds, ct).ConfigureAwait(false);
                        successIds.Clear();
                    }
                }
                else
                {
                    // Produce returned false (circuit may have just opened)
                    entry.RetryCount++;
                    _logger.LogWarning("Replay produce failed for EntryId={Id} (attempt {Retry})",
                        entry.EntryId, entry.RetryCount);
                    aborted = true;
                    break;
                }
            }

            // Flush any remaining success IDs not yet removed
            if (successIds.Count > 0)
                await _fallbackStore.RemoveAsync(successIds, ct).ConfigureAwait(false);

            // If all entries replayed successfully, clear the file
            if (!aborted && !_fallbackStore.HasPendingEntries)
                await _fallbackStore.ClearAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            // Step 2: Signal dispatcher that replay is done — start draining live buffer
            _dispatcher.SetReplayingState(isReplaying: false);

            OnReplayStateChanged?.Invoke(this, new FallbackReplayEventArgs
            {
                IsStarting = false,
                EntriesReplayed = totalReplayed
            });

            _logger.LogInformation("Fallback replay {Result} — replayed {Count} entries",
                aborted ? "ABORTED" : "COMPLETE", totalReplayed);

            // Step 3: Drain buffered live messages (in order, after file entries)
            await _dispatcher.DrainLiveBufferAsync(ct).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
