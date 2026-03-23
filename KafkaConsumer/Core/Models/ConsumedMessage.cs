using System.Text.Json.Serialization;

namespace KafkaConsumer.Core.Models;

// ─────────────────────────────────────────────────────────────────
// Raw payloads arriving from Kafka topics:
//
//  meter_voltage: {"meterId":105,"voltage":221.5,"timestamp":"2026-03-19T07:04:42Z"}
//  meter_current: {"meterId":107,"current":18.3,"timestamp":"2026-03-19T07:04:43Z"}
// ─────────────────────────────────────────────────────────────────

/// <summary>
/// Represents the deserialized JSON payload from either Kafka topic.
/// Voltage and Current are nullable because each topic only sends one of them.
/// </summary>
public sealed class MeterReading
{
    [JsonPropertyName("meterId")]
    public int MeterId { get; init; }

    [JsonPropertyName("voltage")]
    public double? Voltage { get; init; }       // null for current messages

    [JsonPropertyName("current")]
    public double? Current { get; init; }       // null for voltage messages

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Wraps the raw Kafka message with all metadata needed for:
///   - file writing (TopicName, RawJson)
///   - manual offset commit (Partition, Offset)
///   - retry tracking (RetryCount)
///   - ordering guarantee (ReceivedAt, SequenceId)
///
/// This is the single unit of work flowing through the entire pipeline.
/// Immutable except for RetryCount which must increment on each retry.
/// </summary>
public sealed class ConsumedMessage
{
    /// <summary>Internal sequence number — assigned on consume, used for ordering diagnostics.</summary>
    public long SequenceId { get; init; }

    /// <summary>The Kafka topic this message came from.</summary>
    public string TopicName { get; init; } = string.Empty;

    /// <summary>Kafka partition number — needed for manual offset commit.</summary>
    public int Partition { get; init; }

    /// <summary>Kafka offset — needed for manual offset commit.</summary>
    public long Offset { get; init; }

    /// <summary>
    /// The raw JSON string exactly as received from Kafka.
    /// Written as-is to the output file — no transformation.
    /// </summary>
    public string RawJson { get; init; } = string.Empty;

    /// <summary>Deserialized payload — used for logging/metrics only. Writing uses RawJson.</summary>
    public MeterReading? Payload { get; init; }

    /// <summary>UTC time this message was consumed from Kafka.</summary>
    public DateTime ReceivedAt { get; init; } = DateTime.UtcNow;

    /// <summary>How many times file write has been attempted for this message.</summary>
    public int RetryCount { get; set; } = 0;

    /// <summary>The date portion of ReceivedAt — used for daily file naming.</summary>
    public string DateKey => ReceivedAt.ToString("yyyy-MM-dd");
}
