using System.Text.Json.Serialization;

namespace MeterConsumer.Core.Models;

/// <summary>
/// Discriminated union: every message is either Voltage or Current.
/// Source queues: meter_voltage / meter_current in RabbitMQ.
/// </summary>
public enum MeterMessageType
{
    Voltage,
    Current
}

/// <summary>
/// Core domain model — the payload flowing from RabbitMQ → Kafka.
/// All fields are init-only to prevent mutation after construction.
/// </summary>
public sealed class MeterMessage
{
    /// <summary>Unique ID for end-to-end tracing.</summary>
    [JsonPropertyName("id")]
    public Guid Id { get; init; } = Guid.NewGuid();

    /// <summary>Voltage or Current — determines which Kafka topic receives this message.</summary>
    [JsonIgnore]
    [JsonPropertyName("type")]
    public MeterMessageType Type { get; init; }

    /// <summary>Meter device identifier.</summary>
    [JsonPropertyName("meterId")]
    public int MeterId { get; init; }


    /// <summary>Voltage queue sends this.</summary>
    [JsonPropertyName("voltage")]
    public double? Voltage { get; init; }

    /// <summary>Current queue sends this.</summary>
    [JsonPropertyName("current")]
    public double? Current { get; init; }

    /// <summary>UTC time the meter reading was taken.</summary>
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// RabbitMQ delivery tag — needed to ACK/NACK the message.
    /// NOT serialised to Kafka: excluded from JSON output.
    /// </summary>
    [JsonIgnore]
    public ulong DeliveryTag { get; init; }

    /// <summary>
    /// RabbitMQ channel reference — needed to call BasicAckAsync.
    /// NOT serialised to Kafka: excluded from JSON output.
    /// </summary>
    [JsonIgnore]
    public object? ChannelRef { get; init; }
}
