using System.Text.Json.Serialization;

namespace MeterConsumer.Core.Models;

/// <summary>
/// Represents one line in the fallback JSONL file.
/// Contains the full MeterMessage payload plus metadata for replay.
///
/// WHY JSONL?
///  - Append-only writes = no full-file rewrites
///  - Line-by-line reading = no need to load entire file into memory
///  - Human-readable = easy to inspect/debug
///  - Survives partial writes (bad line = skip and continue)
/// </summary>
public sealed class FallbackEntry
{
    /// <summary>Unique entry ID — used to deduplicate during replay.</summary>
    [JsonPropertyName("entryId")]
    public Guid EntryId { get; init; } = Guid.NewGuid();

    /// <summary>UTC time this entry was written to disk.</summary>
    [JsonPropertyName("savedAt")]
    public DateTime SavedAt { get; init; } = DateTime.UtcNow;

    /// <summary>How many Kafka delivery attempts have been made for this entry.</summary>
    [JsonPropertyName("retryCount")]
    public int RetryCount { get; set; } = 0;

    /// <summary>The original meter reading payload.</summary>
    [JsonPropertyName("message")]
    public MeterMessage Message { get; init; } = default!;

    /// <summary>The Kafka topic this entry should be delivered to.</summary>
    [JsonPropertyName("kafkaTopic")]
    public string KafkaTopic { get; init; } = string.Empty;
}
