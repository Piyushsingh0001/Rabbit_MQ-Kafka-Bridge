using System.Text.Json;
using Confluent.Kafka;
using KafkaConsumer.Core.Events;
using KafkaConsumer.Core.Interfaces;
using KafkaConsumer.Core.Models;
using KafkaConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Infrastructure.Kafka;

/// <summary>
/// Polls both Kafka topics and writes ConsumedMessages to the pipeline Channel.
///
/// KEY DESIGN:
///  - Builds and owns its own IConsumer internally — no DI injection
///  - EnableAutoCommit = true: Kafka auto-commits every 5s (reliable, no coordination needed)
///  - Both topics subscribed on one consumer, one poll loop
///  - Exponential backoff reconnect on any Kafka failure
///  - Backpressure: if pipeline Channel is full, WriteAsync suspends this loop
/// </summary>
public sealed class KafkaConsumerService : IKafkaConsumerService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly KafkaSettings _settings;
    private readonly MessagePipeline _pipeline;

    private volatile bool _isConnected = false;
    private long _sequenceCounter = 0;
    private bool _disposed = false;

    public event EventHandler<KafkaConnectionEventArgs>? OnConnectionChanged;
    public event EventHandler<MessageConsumedEventArgs>? OnMessageConsumed;

    public bool IsConnected => _isConnected;

    public KafkaConsumerService(
        IOptions<KafkaConsumerSettings> options,
        MessagePipeline pipeline,
        ILogger<KafkaConsumerService> logger)
    {
        _settings = options.Value.Kafka;
        _pipeline = pipeline;
        _logger = logger;
    }

    // ── Main Entry Point ─────────────────────────────────────────────────────

    public async Task RunAsync(CancellationToken ct)
    {
        int reconnectAttempt = 0;

        while (!ct.IsCancellationRequested)
        {
            IConsumer<string, string>? consumer = null;
            try
            {
                consumer = BuildConsumer();

                // Subscribe to both topics in a single call
                consumer.Subscribe(new[]
                {
                    _settings.VoltageTopicName,
                    _settings.CurrentTopicName
                });

                _isConnected = true;
                reconnectAttempt = 0;

                RaiseConnectionChanged(connected: true, "Connected to Kafka", attempt: 0);

                _logger.LogInformation(
                    "Kafka consumer connected — polling [{V}] and [{C}]",
                    _settings.VoltageTopicName, _settings.CurrentTopicName);

                // Inner poll loop — exits on error or cancellation
                await ConsumeLoopAsync(consumer, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogInformation("Kafka consumer stopping — shutdown requested");
                break;
            }
            catch (KafkaException ex)
            {
                _isConnected = false;
                reconnectAttempt++;

                _logger.LogError(ex,
                    "Kafka error on attempt {Attempt}: {Reason}",
                    reconnectAttempt, ex.Error.Reason);

                RaiseConnectionChanged(
                    connected: false,
                    $"Kafka error: {ex.Error.Reason}",
                    attempt: reconnectAttempt);

                if (_settings.MaxReconnectAttempts > 0
                    && reconnectAttempt >= _settings.MaxReconnectAttempts)
                {
                    _logger.LogCritical("Max reconnect attempts reached — giving up");
                    break;
                }

                // Exponential backoff: 2s → 4s → 8s → max 60s
                var delayMs = Math.Min(
                    _settings.ReconnectBaseDelayMs * Math.Pow(2, reconnectAttempt - 1),
                    _settings.ReconnectMaxDelayMs);

                _logger.LogWarning("Reconnecting in {Delay}ms...", delayMs);
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _isConnected = false;
                _logger.LogError(ex, "Unexpected Kafka error — reconnecting");
                await Task.Delay(TimeSpan.FromMilliseconds(_settings.ReconnectBaseDelayMs), ct)
                    .ConfigureAwait(false);
            }
            finally
            {
                SafeClose(consumer);
            }
        }

        // Tell FileWriterService no more messages are coming — it will drain and exit
        _pipeline.Complete();
        _logger.LogInformation("Kafka consumer stopped — pipeline marked complete");
    }

    // ── Poll Loop ─────────────────────────────────────────────────────────────

    private async Task ConsumeLoopAsync(IConsumer<string, string> consumer, CancellationToken ct)
    {
        var pollTimeout = TimeSpan.FromMilliseconds(_settings.PollTimeoutMs);

        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            
            try
            {
                // consumer.Consume() is synchronous by Confluent design
                // runs fast (microseconds if message available, PollTimeout if empty)
                result = consumer.Consume(pollTimeout);
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex, "Consume error: {Reason}", ex.Error.Reason);

                if (!ex.Error.IsFatal) continue;         // Non-fatal — keep polling
                throw new KafkaException(ex.Error);      // Fatal — trigger reconnect
            }

            // null = no messages arrived within PollTimeout — normal, keep polling
            if (result?.Message == null)
                continue;

            var message = BuildMessage(result);

            // Write to pipeline Channel.
            // BACKPRESSURE: if Channel is full, WriteAsync suspends here.
            // FileWriterService will catch up — no message dropped, no spin.
            await _pipeline.Writer
                .WriteAsync(message, ct)
                .ConfigureAwait(false);

            // Raise event — KafkaWorker logs consumed message details
            OnMessageConsumed?.Invoke(this, new MessageConsumedEventArgs
            {
                SequenceId = message.SequenceId,
                TopicName  = message.TopicName,
                Partition  = message.Partition,
                Offset     = message.Offset,
                MeterId    = message.Payload?.MeterId ?? 0
            });
        }
    }

    // ── Build Consumer ────────────────────────────────────────────────────────

    private IConsumer<string, string> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers    = _settings.BootstrapServers,
            GroupId             = _settings.GroupId,

            // EnableAutoCommit = true:
            // Kafka commits offsets automatically every 5 seconds.
            // FileWriterService is fully decoupled — no commit coordination needed.
            // On restart: resumes from last auto-committed offset.
            EnableAutoCommit    = true,
            AutoCommitIntervalMs = 5000,

            // Earliest: if no offset committed yet, start from beginning of topic
            AutoOffsetReset     = _settings.AutoOffsetReset.ToLowerInvariant() == "latest"
                ? AutoOffsetReset.Latest
                : AutoOffsetReset.Earliest,

            SessionTimeoutMs    = 30_000,
            MaxPartitionFetchBytes = 1_048_576   // 1 MB per partition per fetch
        };

        return new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("Partitions assigned: {P}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation("Partitions revoked: {P}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetErrorHandler((_, e) =>
            {
                if (e.IsFatal)
                    _logger.LogCritical("FATAL Kafka error: {Reason}", e.Reason);
                else
                    _logger.LogWarning("Kafka warning: {Reason}", e.Reason);
            })
            .Build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ConsumedMessage BuildMessage(ConsumeResult<string, string> result)
    {
        MeterReading? payload = null;
        try
        {
            payload = JsonSerializer.Deserialize<MeterReading>(result.Message.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex,
                "Invalid JSON from {Topic}[{P}]@{O} — RawJson will still be written to file",
                result.Topic, result.Partition.Value, result.Offset.Value);
        }

        return new ConsumedMessage
        {
            SequenceId  = Interlocked.Increment(ref _sequenceCounter),
            TopicName   = result.Topic,
            Partition   = result.Partition.Value,
            Offset      = result.Offset.Value,
            RawJson     = result.Message.Value ?? string.Empty,
            Payload     = payload,
            ReceivedAt  = DateTime.UtcNow
        };
    }

    private void SafeClose(IConsumer<string, string>? consumer)
    {
        if (consumer == null) return;
        try
        {
            consumer.Unsubscribe();
            consumer.Close();       // commits final auto-offsets before closing
            consumer.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while closing Kafka consumer");
        }
    }

    private void RaiseConnectionChanged(bool connected, string reason, int attempt) =>
        OnConnectionChanged?.Invoke(this, new KafkaConnectionEventArgs
        {
            IsConnected       = connected,
            Reason            = reason,
            ReconnectAttempt  = attempt
        });

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        _pipeline.Complete();
        return ValueTask.CompletedTask;
    }
}
