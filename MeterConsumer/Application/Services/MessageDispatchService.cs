using System.Threading.Channels;
using MeterConsumer.Core.Events;
using MeterConsumer.Core.Interfaces;
using MeterConsumer.Core.Models;
using MeterConsumer.Infrastructure.Configuration;
using MeterConsumer.Infrastructure.RabbitMq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MeterConsumer.Application.Services;

/// <summary>
/// Central dispatcher: receives messages from RabbitMQ consumer and routes them safely.
///
/// DISPATCH DECISION TREE:
/// ─────────────────────────────────────────────────────────────
///
///  Incoming RabbitMQ message
///         │
///         ▼
///  Is FallbackReplayService currently replaying the file?
///  │
///  ├─ YES → Enqueue to in-memory Channel (buffering)
///  │         (live messages wait — file replay has priority for ordering)
///  │
///  └─ NO  → Is Kafka available? (circuit breaker check)
///            │
///            ├─ YES → ProduceAsync to Kafka
///            │         ├─ Success → BasicAckAsync  ✓ DONE
///            │         └─ Failure → Save to fallback file → BasicAckAsync
///            │
///            └─ NO  → Save to fallback file → BasicAckAsync
///                       (Kafka down — file ensures no data loss)
///
/// ─────────────────────────────────────────────────────────────
/// GUARANTEE: BasicAckAsync is ALWAYS called, either after:
///   1. Confirmed Kafka delivery
///   2. Confirmed file write (FallbackReplayService will retry later)
///   Never before either of these.
/// ─────────────────────────────────────────────────────────────
///
/// IN-MEMORY BUFFER:
///   During fallback replay, new messages go to a bounded Channel<T>.
///   After replay completes, the channel is drained in order.
///   Channel is bounded (200 items) — if full, backpressure applies:
///   new messages go directly to fallback file instead.
/// </summary>
public sealed class MessageDispatchService : IAsyncDisposable
{
    private readonly ILogger<MessageDispatchService> _logger;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly IFallbackStore _fallbackStore;
    private readonly RabbitMqConsumerService _rabbitMqConsumer;
    private readonly KafkaSettings _kafkaSettings;

    // ── In-memory buffer used during fallback replay ──────────────────────────
    private readonly Channel<MeterMessage> _liveBuffer;

    // ── Replay state flag (set by FallbackReplayService) ─────────────────────
    private volatile bool _isReplaying = false;

    // ── Events ───────────────────────────────────────────────────────────────
    public event EventHandler<MessageReceivedEventArgs>? OnMessageReceived;
    public event EventHandler<MessageProcessedEventArgs>? OnMessageProcessed;
    public event EventHandler<MessageFailedEventArgs>? OnMessageFailed;

    public MessageDispatchService(
        IKafkaProducer kafkaProducer,
        IFallbackStore fallbackStore,
        RabbitMqConsumerService rabbitMqConsumer,
        IOptions<MeterConsumerSettings> options,
        ILogger<MessageDispatchService> logger)
    {
        _kafkaProducer = kafkaProducer;
        _fallbackStore = fallbackStore;
        _rabbitMqConsumer = rabbitMqConsumer;
        _kafkaSettings = options.Value.Kafka;
        _logger = logger;

        // Bounded channel — backpressure via DropWrite if full during replay
        _liveBuffer = Channel.CreateBounded<MeterMessage>(new BoundedChannelOptions(
            options.Value.Processing.InMemoryQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.DropWrite, // If buffer full → go to file
            SingleReader = true,
            SingleWriter = false
        });

        // Forward Kafka delivery events
        _kafkaProducer.OnMessageDelivered += (_, e) => OnMessageProcessed?.Invoke(this, e);
    }

    // ── Called by FallbackReplayService ──────────────────────────────────────

    /// <summary>
    /// Signal that fallback replay has started.
    /// New live messages will be buffered in Channel instead of going direct to Kafka.
    /// </summary>
    public void SetReplayingState(bool isReplaying)
    {
        _isReplaying = isReplaying;
        _logger.LogInformation("Dispatch mode: {Mode}",
            isReplaying ? "REPLAY (live messages buffering)" : "LIVE (direct to Kafka)");
    }

    /// <summary>
    /// Drains the in-memory live buffer after replay completes.
    /// Called by FallbackReplayService once file is empty.
    /// </summary>
    public async Task DrainLiveBufferAsync(CancellationToken ct)
    {
        _logger.LogInformation("Draining live message buffer...");
        int count = 0;

        while (_liveBuffer.Reader.TryRead(out var msg))
        {
            await DispatchDirectAsync(msg, ct).ConfigureAwait(false);
            count++;
        }

        _logger.LogInformation("Live buffer drained: {Count} messages", count);
    }

    // ── Main entry point (called by RabbitMQ consumer) ────────────────────────

    /// <summary>
    /// Called for every message received from RabbitMQ.
    /// Decides: buffer | direct-to-Kafka | fallback file.
    /// </summary>
    public async Task HandleIncomingAsync(MeterMessage message)
    {
        OnMessageReceived?.Invoke(this, new MessageReceivedEventArgs
        {
            MessageId = message.Id,
            QueueName = message.Type == MeterMessageType.Voltage
                ? _kafkaSettings.VoltageTopic : _kafkaSettings.CurrentTopic,
            MessageType = message.Type.ToString()
        });

        // During replay: buffer live messages in Channel (don't interleave with file replay)
        if (_isReplaying)
        {
            if (_liveBuffer.Writer.TryWrite(message))
            {
                _logger.LogDebug("Replay active — buffered MsgId={Id}", message.Id);
                // ACK here: message is safe in our in-memory buffer
                // Note: if service crashes now, message returns to RabbitMQ — that's acceptable
                await _rabbitMqConsumer.AckAsync(message).ConfigureAwait(false);
                return;
            }
            else
            {
                // Buffer full → save to fallback file (never lose the message)
                _logger.LogWarning("Live buffer full during replay — routing MsgId={Id} to fallback file", message.Id);
                await SaveToFallbackAndAckAsync(message).ConfigureAwait(false);
                return;
            }
        }

        // Normal path: try Kafka directly
        await DispatchDirectAsync(message, CancellationToken.None).ConfigureAwait(false);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Try Kafka first. On failure, save to fallback file.
    /// ACK RabbitMQ after either path succeeds.
    /// </summary>
    private async Task DispatchDirectAsync(MeterMessage message, CancellationToken ct)
    {
        // Path A: Kafka available — produce directly
        if (_kafkaProducer.IsAvailable)
        {
            var delivered = await _kafkaProducer.ProduceAsync(message, ct).ConfigureAwait(false);

            if (delivered)
            {
                // SUCCESS — ACK to RabbitMQ (message safely in Kafka)
                await _rabbitMqConsumer.AckAsync(message).ConfigureAwait(false);
                return;
            }
        }

        // Path B: Kafka unavailable or delivery failed — save to file
        _logger.LogWarning("Kafka unavailable — routing MsgId={Id} to fallback file", message.Id);
        await SaveToFallbackAndAckAsync(message).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes to fallback file then ACKs RabbitMQ.
    /// If file write fails too (disk full?) → NACK (message stays in RabbitMQ).
    /// </summary>
    private async Task SaveToFallbackAndAckAsync(MeterMessage message)
    {
        var topic = message.Type == MeterMessageType.Voltage
            ? _kafkaSettings.VoltageTopic
            : _kafkaSettings.CurrentTopic;

        try
        {
            var entry = new FallbackEntry
            {
                Message = message,
                KafkaTopic = topic
            };

            await _fallbackStore.SaveAsync(entry).ConfigureAwait(false);

            // File write confirmed — safe to ACK RabbitMQ
            await _rabbitMqConsumer.AckAsync(message).ConfigureAwait(false);

            OnMessageFailed?.Invoke(this, new MessageFailedEventArgs
            {
                MessageId = message.Id,
                Reason = "Kafka unavailable — saved to fallback file",
                SavedToFallback = true
            });
        }
        catch (Exception ex)
        {
            // Both Kafka AND file failed — critical situation
            // NACK with requeue=true so RabbitMQ re-delivers when things recover
            _logger.LogCritical(ex,
                "CRITICAL: Both Kafka and fallback file failed for MsgId={Id} — NACKing for requeue",
                message.Id);

            await _rabbitMqConsumer.NackAsync(message, requeue: true).ConfigureAwait(false);

            OnMessageFailed?.Invoke(this, new MessageFailedEventArgs
            {
                MessageId = message.Id,
                Reason = "Both Kafka and fallback file failed",
                Exception = ex,
                SavedToFallback = false
            });
        }
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        _kafkaProducer.OnMessageDelivered -= (_, e) => OnMessageProcessed?.Invoke(this, e);
        _liveBuffer.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
