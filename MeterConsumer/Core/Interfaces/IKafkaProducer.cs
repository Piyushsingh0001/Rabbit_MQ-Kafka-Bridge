using MeterConsumer.Core.Events;
using MeterConsumer.Core.Models;

namespace MeterConsumer.Core.Interfaces;

/// <summary>
/// Abstraction over Kafka producer.
/// Implementations handle: topic routing, delivery confirmation, circuit breaker.
/// </summary>
public interface IKafkaProducer : IAsyncDisposable
{
    // ── Events ──────────────────────────────────────────────
    event EventHandler<MessageProcessedEventArgs>? OnMessageDelivered;
    event EventHandler<ConnectionStatusChangedEventArgs>? OnConnectionStatusChanged;

    // ── Properties ──────────────────────────────────────────
    /// <summary>True = circuit closed = Kafka accepting messages.</summary>
    bool IsAvailable { get; }

    // ── Methods ─────────────────────────────────────────────

    /// <summary>
    /// Ensures the required Kafka topics exist; creates them if missing.
    /// Called once at startup.
    /// </summary>
    Task EnsureTopicsExistAsync(CancellationToken ct = default);

    /// <summary>
    /// Produces a message to the appropriate Kafka topic.
    /// Returns true on confirmed delivery, false on failure (circuit open or Kafka error).
    /// NEVER throws — returns false and logs instead.
    /// </summary>
    Task<bool> ProduceAsync(MeterMessage message, CancellationToken ct = default);
}
