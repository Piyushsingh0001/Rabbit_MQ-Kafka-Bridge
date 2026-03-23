namespace KafkaConsumer.Infrastructure.Configuration;

/// <summary>Root settings — maps to "KafkaConsumerSettings" in appsettings.json.</summary>
public sealed class KafkaConsumerSettings
{
    public const string Section = "KafkaConsumerSettings";

    public KafkaSettings Kafka { get; set; } = new();
    public FileWriterSettings FileWriter { get; set; } = new();
    public ProcessingSettings Processing { get; set; } = new();
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "MeterReaderGroup";
    public string VoltageTopicName { get; set; } = "meter_voltage";
    public string CurrentTopicName { get; set; } = "meter_current";

    /// <summary>
    /// "Earliest" = start from beginning if no committed offset exists.
    /// "Latest"   = start from newest messages only.
    /// </summary>
    public string AutoOffsetReset { get; set; } = "Earliest";

    /// <summary>
    /// ALWAYS false — we commit manually only after confirmed file write.
    /// This property exists only as documentation; code enforces false.
    /// </summary>
    public bool EnableAutoCommit => false;

    public int MaxPollMessages { get; set; } = 100;
    public int PollTimeoutMs { get; set; } = 1000;

    /// <summary>Base delay for exponential backoff reconnect (ms).</summary>
    public int ReconnectBaseDelayMs { get; set; } = 2000;

    /// <summary>Max delay cap for reconnect backoff (ms). Default 60s.</summary>
    public int ReconnectMaxDelayMs { get; set; } = 60_000;

    /// <summary>0 = retry forever until cancellation.</summary>
    public int MaxReconnectAttempts { get; set; } = 0;
}

public sealed class FileWriterSettings
{
    public string OutputDirectory { get; set; } = @"C:\ProgramData\KafkaConsumer\Output";

    /// <summary>Messages accumulated before a file flush is triggered.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>
    /// Max ms before flushing even if batch is not full.
    /// Prevents stale data sitting in memory during low-traffic periods.
    /// </summary>
    public int FlushIntervalMs { get; set; } = 5_000;

    /// <summary>Max in-memory retry queue capacity. Bounded to prevent memory growth.</summary>
    public int RetryQueueCapacity { get; set; } = 500;

    /// <summary>Max attempts before a batch is permanently failed.</summary>
    public int MaxFileWriteRetries { get; set; } = 5;

    /// <summary>Base delay between file write retries (ms).</summary>
    public int FileWriteRetryDelayMs { get; set; } = 2_000;
}

public sealed class ProcessingSettings
{
    /// <summary>
    /// Bounded Channel<T> capacity between consumer and writer.
    /// Provides backpressure: if writer is slow, consumer slows down.
    /// </summary>
    public int ChannelCapacity { get; set; } = 1_000;

    public int HealthLogIntervalSeconds { get; set; } = 30;
}
