using System.Threading.Channels;
using KafkaConsumer.Core.Models;
using KafkaConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Infrastructure.Kafka;

/// <summary>
/// The bounded Channel that connects the Kafka consumer to the file writer.
///
/// WHY A CHANNEL?
/// ─────────────────────────────────────────────────────────────────
///  • Producer (KafkaConsumerService) writes messages as they arrive
///  • Consumer (FileWriterService) reads and batches messages for disk
///  • Bounded capacity provides BACKPRESSURE:
///    If FileWriter is slow (disk busy), the Channel fills up.
///    KafkaConsumerService awaits WriteAsync — suspending the poll loop.
///    This prevents unbounded memory growth during write slowdowns.
///
///  • Lock-free internals — high throughput with minimal contention
///  • Async wait semantics — no busy-polling on either end
///
/// SINGLETON: One pipeline instance shared by both services.
/// Registered in DI as a singleton so both sides reference the same instance.
/// </summary>
public sealed class MessagePipeline : IDisposable
{
    private readonly Channel<ConsumedMessage> _channel;

    public MessagePipeline(IOptions<KafkaConsumerSettings> options)
    {
        var capacity = options.Value.Processing.ChannelCapacity;

        _channel = Channel.CreateBounded<ConsumedMessage>(new BoundedChannelOptions(capacity)
        {
            // WAIT = if channel is full, WriteAsync suspends the Kafka poll loop.
            // This is the backpressure mechanism — writer slows down reader automatically.
            FullMode = BoundedChannelFullMode.Wait,

            // Single reader (FileWriterService) — enables internal optimizations
            SingleReader = true,

            // Single writer (KafkaConsumerService poll loop)
            SingleWriter = true,

            // Disable synchronous continuations to prevent stack overflows in hot loops
            AllowSynchronousContinuations = false
        });
    }

    /// <summary>Write end — used by KafkaConsumerService.</summary>
    public ChannelWriter<ConsumedMessage> Writer => _channel.Writer;

    /// <summary>Read end — used by FileWriterService.</summary>
    public ChannelReader<ConsumedMessage> Reader => _channel.Reader;

    /// <summary>Approximate number of messages currently buffered.</summary>
    public int Count => _channel.Reader.Count;

    /// <summary>
    /// Called on shutdown. Signals FileWriterService that no more messages will arrive.
    /// FileWriterService will drain remaining messages then exit its read loop.
    /// </summary>
    public void Complete() => _channel.Writer.TryComplete();

    public void Dispose() => Complete();
}
