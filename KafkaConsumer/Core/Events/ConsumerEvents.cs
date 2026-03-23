namespace KafkaConsumer.Core.Events;

// All EventArgs are sealed + init-only = immutable after construction.
// Safe to pass across threads without locks.

/// <summary>Raised when a message is successfully consumed from Kafka.</summary>
public sealed class MessageConsumedEventArgs : EventArgs
{
    public long SequenceId   { get; init; }
    public string TopicName  { get; init; } = string.Empty;
    public int Partition     { get; init; }
    public long Offset       { get; init; }
    public int MeterId       { get; init; }
    public DateTime ConsumedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>Raised when a message batch is successfully written to the output file.</summary>
public sealed class BatchWrittenEventArgs : EventArgs
{
    public string TopicName   { get; init; } = string.Empty;
    public string FilePath    { get; init; } = string.Empty;
    public int MessageCount   { get; init; }
    public long FirstOffset   { get; init; }
    public long LastOffset    { get; init; }
    public DateTime WrittenAt { get; init; } = DateTime.UtcNow;
}

/// <summary>Raised when a file write attempt fails — before retry.</summary>
public sealed class FileWriteFailedEventArgs : EventArgs
{
    public string TopicName   { get; init; } = string.Empty;
    public string FilePath    { get; init; } = string.Empty;
    public int MessageCount   { get; init; }
    public int RetryAttempt   { get; init; }
    public Exception? Exception { get; init; }
    public bool WillRetry     { get; init; }
    public DateTime FailedAt  { get; init; } = DateTime.UtcNow;
}

/// <summary>Raised when Kafka connection state changes.</summary>
public sealed class KafkaConnectionEventArgs : EventArgs
{
    public bool IsConnected      { get; init; }
    public string Reason         { get; init; } = string.Empty;
    public int ReconnectAttempt  { get; init; }
    public DateTime ChangedAt    { get; init; } = DateTime.UtcNow;
}

/// <summary>Raised when a message enters the retry queue after file write failure.</summary>
public sealed class MessageRetryQueuedEventArgs : EventArgs
{
    public long SequenceId      { get; init; }
    public string TopicName     { get; init; } = string.Empty;
    public int RetryCount       { get; init; }
    public int RetryQueueDepth  { get; init; }
    public DateTime QueuedAt    { get; init; } = DateTime.UtcNow;
}
