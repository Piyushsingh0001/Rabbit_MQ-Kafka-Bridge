using KafkaConsumer.Core.Events;

namespace KafkaConsumer.Core.Interfaces;

/// <summary>
/// Abstraction over the Kafka consumer.
/// Owns the IConsumer internally — no other service touches Kafka directly.
/// </summary>
public interface IKafkaConsumerService : IAsyncDisposable
{
    event EventHandler<KafkaConnectionEventArgs>? OnConnectionChanged;
    event EventHandler<MessageConsumedEventArgs>? OnMessageConsumed;

    /// <summary>True when consumer is connected and actively polling.</summary>
    bool IsConnected { get; }

    /// <summary>
    /// Polls both Kafka topics, pushes messages to the pipeline Channel.
    /// Runs until cancellation. Reconnects automatically on Kafka failure.
    /// </summary>
    Task RunAsync(CancellationToken ct);
}

/// <summary>
/// Abstraction over the file writer.
/// Reads from the pipeline Channel and writes JSON lines to daily files.
/// Has ZERO dependency on Kafka — completely decoupled.
/// </summary>
public interface IFileWriterService : IAsyncDisposable
{
    event EventHandler<BatchWrittenEventArgs>? OnBatchWritten;
    event EventHandler<FileWriteFailedEventArgs>? OnWriteFailed;
    event EventHandler<MessageRetryQueuedEventArgs>? OnMessageRetryQueued;

    /// <summary>Current depth of the in-memory retry queue.</summary>
    int RetryQueueDepth { get; }

    /// <summary>
    /// Reads from pipeline, batches writes to daily topic files.
    /// Retries on file failure. Runs until pipeline is complete and drained.
    /// </summary>
    Task RunAsync(CancellationToken ct);
}
