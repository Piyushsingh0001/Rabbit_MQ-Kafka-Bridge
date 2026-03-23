namespace MeterConsumer.Infrastructure.Configuration;

/// <summary>Root settings class — maps to "MeterConsumerSettings" in appsettings.json.</summary>
public sealed class MeterConsumerSettings
{
    public const string Section = "MeterConsumerSettings";

    public RabbitMqSettings RabbitMq { get; set; } = new();
    public KafkaSettings Kafka { get; set; } = new();
    public FallbackSettings Fallback { get; set; } = new();
    public ProcessingSettings Processing { get; set; } = new();
}

public sealed class RabbitMqSettings
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string Username { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";

    /// <summary>RabbitMQ queue for voltage messages (already exists — durable, classic).</summary>
    public string VoltageQueue { get; set; } = "meter_voltage";

    /// <summary>RabbitMQ queue for current messages (already exists — durable, classic).</summary>
    public string CurrentQueue { get; set; } = "meter_current";

    /// <summary>
    /// Max unacknowledged messages the broker sends to this consumer.
    /// Provides RabbitMQ-side backpressure.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string VoltageTopic { get; set; } = "meter_voltage";
    public string CurrentTopic { get; set; } = "meter_current";
    public int TopicPartitions { get; set; } = 3;
    public short TopicReplicationFactor { get; set; } = 1;

    /// <summary>How many consecutive failures before circuit opens.</summary>
    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    /// <summary>Seconds before circuit moves from Open to HalfOpen (probe attempt).</summary>
    public int CircuitBreakerResetSeconds { get; set; } = 30;
}

public sealed class FallbackSettings
{
    /// <summary>Full path to the JSONL fallback file.</summary>
    public string FilePath { get; set; } = @"C:\ProgramData\MeterConsumer\fallback.jsonl";

    /// <summary>Stop writing to file if it exceeds this size (100 MB default).</summary>
    public long MaxFileSizeBytes { get; set; } = 104_857_600; // 100 MB

    /// <summary>How many entries to replay per Kafka batch during recovery.</summary>
    public int ReplayBatchSize { get; set; } = 50;

    /// <summary>How often (seconds) the replay loop checks for pending entries.</summary>
    public int ReplayIntervalSeconds { get; set; } = 10;
}

public sealed class ProcessingSettings
{
    /// <summary>SemaphoreSlim limit — max parallel Kafka produce calls.</summary>
    public int MaxConcurrentMessages { get; set; } = 4;

    /// <summary>Channel capacity for live messages buffered during fallback replay.</summary>
    public int InMemoryQueueCapacity { get; set; } = 200;
}
