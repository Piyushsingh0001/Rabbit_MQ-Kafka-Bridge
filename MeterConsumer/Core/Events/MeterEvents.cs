namespace MeterConsumer.Core.Events;

// ─────────────────────────────────────────────────────────────
// EventArgs — immutable, init-only for thread-safety
// ─────────────────────────────────────────────────────────────

/// <summary>
/// Raised when a meter message is received from RabbitMQ.
/// </summary>
public sealed class MessageReceivedEventArgs : EventArgs
{
    public Guid MessageId { get; init; }
    public string QueueName { get; init; } = string.Empty;
    public string MessageType { get; init; } = string.Empty;
    public DateTime ReceivedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Raised when a message has been successfully delivered to Kafka.
/// </summary>
public sealed class MessageProcessedEventArgs : EventArgs
{
    public Guid MessageId { get; init; }
    public string KafkaTopic { get; init; } = string.Empty;
    public int Partition { get; init; }
    public long Offset { get; init; }
    public DateTime ProcessedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Raised when processing fails (Kafka down, validation error, etc.).
/// </summary>
public sealed class MessageFailedEventArgs : EventArgs
{
    public Guid MessageId { get; init; }
    public string Reason { get; init; } = string.Empty;
    public Exception? Exception { get; init; }
    public bool SavedToFallback { get; init; }
    public DateTime FailedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Raised when Kafka connection state changes (circuit breaker trips/resets).
/// </summary>
public sealed class ConnectionStatusChangedEventArgs : EventArgs
{
    public string ServiceName { get; init; } = string.Empty;   // "Kafka" or "RabbitMQ"
    public bool IsConnected { get; init; }
    public string Reason { get; init; } = string.Empty;
    public DateTime ChangedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Raised when fallback file replay starts or finishes.
/// </summary>
public sealed class FallbackReplayEventArgs : EventArgs
{
    public bool IsStarting { get; init; }           // true=starting, false=completed
    public int EntriesReplayed { get; init; }
    public DateTime At { get; init; } = DateTime.UtcNow;
}
