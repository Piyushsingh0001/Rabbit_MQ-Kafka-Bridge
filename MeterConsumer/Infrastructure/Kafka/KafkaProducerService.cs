using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MeterConsumer.Core.Events;
using MeterConsumer.Core.Interfaces;
using MeterConsumer.Core.Models;
using MeterConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace MeterConsumer.Infrastructure.Kafka;

/// <summary>
/// Confluent Kafka producer implementation.
///
/// Key design decisions:
///  1. Topic auto-creation: AdminClient checks and creates topics at startup
///  2. Circuit breaker: wraps every produce call — no flood of failed requests to dead Kafka
///  3. Delivery callback: ACK is only confirmed when broker reports success (not fire-and-forget)
///  4. SemaphoreSlim: limits concurrent Kafka calls per MaxConcurrentMessages
///  5. DisposeAsync: flushes in-flight messages before shutdown
/// </summary>
public sealed class KafkaProducerService : IKafkaProducer
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly KafkaSettings _settings;
    private readonly KafkaCircuitBreaker _circuitBreaker;
    private readonly IProducer<string, string> _producer;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed;

    // ── Events ───────────────────────────────────────────────────────────────
    public event EventHandler<MessageProcessedEventArgs>? OnMessageDelivered;
    public event EventHandler<ConnectionStatusChangedEventArgs>? OnConnectionStatusChanged;

    // ── Constructor ──────────────────────────────────────────────────────────
    public KafkaProducerService(
        IOptions<MeterConsumerSettings> options,
        ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _settings = options.Value.Kafka;

        _circuitBreaker = new KafkaCircuitBreaker(
            _settings.CircuitBreakerFailureThreshold,
            _settings.CircuitBreakerResetSeconds);

        // Forward circuit breaker events
        _circuitBreaker.OnStatusChanged += (_, e) => OnConnectionStatusChanged?.Invoke(this, e);

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            // Wait for all in-sync replicas to acknowledge — strongest durability guarantee
            Acks = Acks.All,
            // Retry up to 3 times on transient network errors
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            // Enable idempotence: exactly-once delivery per session
            EnableIdempotence = true,
            // Linger allows micro-batching (5ms) without sacrificing much latency
            LingerMs = 5,
            // Message timeout: fail fast rather than queue forever
            MessageTimeoutMs = 30_000
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
        _semaphore = new SemaphoreSlim(options.Value.Processing.MaxConcurrentMessages);
    }

    // ── IKafkaProducer ───────────────────────────────────────────────────────

    public bool IsAvailable => _circuitBreaker.IsAvailable;

    /// <summary>
    /// Creates topics if they don't exist.
    /// Uses AdminClient (separate connection) — does not block the producer.
    /// </summary>
    public async Task EnsureTopicsExistAsync(CancellationToken ct = default)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _settings.BootstrapServers };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var requiredTopics = new[]
        {
            _settings.VoltageTopic,
            _settings.CurrentTopic
        };

        // Get existing topics from Kafka metadata
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet(StringComparer.OrdinalIgnoreCase);

        var topicsToCreate = requiredTopics
            .Where(t => !existingTopics.Contains(t))
            .Select(t => new TopicSpecification
            {
                Name = t,
                NumPartitions = _settings.TopicPartitions,
                ReplicationFactor = _settings.TopicReplicationFactor
            })
            .ToList();

        if (topicsToCreate.Count == 0)
        {
            _logger.LogInformation("Kafka topics already exist: {Topics}", string.Join(", ", requiredTopics));
            return;
        }

        try
        {
            await adminClient.CreateTopicsAsync(topicsToCreate).ConfigureAwait(false);
            foreach (var spec in topicsToCreate)
                _logger.LogInformation("Created Kafka topic '{Topic}' with {Partitions} partitions", spec.Name, spec.NumPartitions);
        }
        catch (CreateTopicsException ex)
        {
            // TOPIC_ALREADY_EXISTS is not an error — it means a race condition where
            // another instance created it first. Log and continue.
            foreach (var result in ex.Results)
            {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                    _logger.LogInformation("Kafka topic '{Topic}' already exists (race condition — OK)", result.Topic);
                else
                    _logger.LogError("Failed to create Kafka topic '{Topic}': {Error}", result.Topic, result.Error.Reason);
            }
        }
    }

    /// <summary>
    /// Produces a message to Kafka.
    /// Returns true only after confirmed broker acknowledgment.
    /// Never throws — returns false and records failure instead.
    /// </summary>
    public async Task<bool> ProduceAsync(MeterMessage message, CancellationToken ct = default)
    {
        // Circuit open → fail fast, do not attempt
        if (!_circuitBreaker.IsAvailable)
        {
            _logger.LogWarning("Kafka circuit is OPEN — skipping produce for Message {Id}", message.Id);
            return false;
        }

        var topic = message.Type == MeterMessageType.Voltage
            ? _settings.VoltageTopic
            : _settings.CurrentTopic;

        // Serialize the message payload (excluding ACK fields via [JsonIgnore])
        var payload = JsonSerializer.Serialize(message);

        // Throttle concurrent produce calls
        await _semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var kafkaMessage = new Message<string, string>
            {
                // Use MeterId as partition key → same meter always lands on same partition (ordering)
                Key = message.MeterId.ToString(),
                Value = payload
            };

            // ProduceAsync waits for broker ACK (Acks.All) — confirmed delivery
            var result = await _producer
                .ProduceAsync(topic, kafkaMessage, ct)
                .ConfigureAwait(false);

            _circuitBreaker.RecordSuccess();

            _logger.LogInformation(
                "Kafka delivered | MsgId={Id} Topic={Topic} Partition={P} Offset={O}",
                message.Id, topic, result.Partition.Value, result.Offset.Value);

            OnMessageDelivered?.Invoke(this, new MessageProcessedEventArgs
            {
                MessageId = message.Id,
                KafkaTopic = topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value
            });

            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            _circuitBreaker.RecordFailure();
            _logger.LogError(ex, "Kafka produce FAILED | MsgId={Id} Topic={Topic} Error={Error}",
                message.Id, topic, ex.Error.Reason);
            return false;
        }
        catch (Exception ex)
        {
            _circuitBreaker.RecordFailure();
            _logger.LogError(ex, "Kafka produce UNEXPECTED ERROR | MsgId={Id}", message.Id);
            return false;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    // ── Disposal ─────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Unsubscribe events to prevent leaks
        _circuitBreaker.OnStatusChanged -= (_, e) => OnConnectionStatusChanged?.Invoke(this, e);

        // Flush remaining in-flight messages (up to 10 seconds)
        await Task.Run(() => _producer.Flush(TimeSpan.FromSeconds(10))).ConfigureAwait(false);
        _producer.Dispose();
        _semaphore.Dispose();

        _logger.LogInformation("KafkaProducerService disposed");
    }
}
