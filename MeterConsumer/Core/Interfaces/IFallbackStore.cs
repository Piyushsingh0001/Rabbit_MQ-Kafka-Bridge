using MeterConsumer.Core.Models;

namespace MeterConsumer.Core.Interfaces;

/// <summary>
/// Abstraction over the local disk fallback store.
/// Implementations must be thread-safe — multiple threads may call simultaneously.
/// </summary>
public interface IFallbackStore
{
    /// <summary>True if there are pending entries waiting for replay.</summary>
    bool HasPendingEntries { get; }

    /// <summary>Count of pending entries (approximate — file-based).</summary>
    int PendingCount { get; }

    /// <summary>
    /// Appends an entry to the fallback file.
    /// Called when Kafka is unavailable so the message is not lost.
    /// ACK to RabbitMQ happens AFTER this returns successfully.
    /// </summary>
    Task SaveAsync(FallbackEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Reads all pending entries as an async stream.
    /// Used by FallbackReplayService to drain the file to Kafka.
    /// </summary>
    IAsyncEnumerable<FallbackEntry> ReadAllAsync(CancellationToken ct = default);

    /// <summary>
    /// Removes successfully replayed entries from the file.
    /// Only called after confirmed Kafka delivery — never before.
    /// </summary>
    Task RemoveAsync(IEnumerable<Guid> entryIds, CancellationToken ct = default);

    /// <summary>
    /// Clears the entire fallback file. Called after a full successful replay.
    /// </summary>
    Task ClearAsync(CancellationToken ct = default);
}
