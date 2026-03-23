using MeterConsumer.Core.Events;
using MeterConsumer.Core.Models;
using MeterConsumer.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace MeterConsumer.Infrastructure.RabbitMq;

/// <summary>
/// Async RabbitMQ consumer for meter_voltage and meter_current queues.
///
/// KEY DESIGN: MANUAL ACKNOWLEDGE ONLY
/// ─────────────────────────────────────────────────────────────
/// autoAck = false (always)
/// Message is NOT acknowledged until:
///   - Kafka delivery confirmed    → BasicAckAsync
///   - OR Fallback file written    → BasicAckAsync  (message is now safe on disk)
///   - OR Validation fails         → BasicAckAsync  (invalid — discard safely)
///
/// If service crashes BEFORE ack, RabbitMQ re-delivers on restart.
///
/// PREFETCH:
/// QoS prefetch limits how many unacked messages the broker sends us.
/// This provides server-side backpressure — broker won't flood us.
///
/// RECONNECTION:
/// If RabbitMQ connection drops, the consumer raises OnConnectionLost.
/// The Worker re-subscribes by calling StartAsync again.
/// </summary>
public sealed class RabbitMqConsumerService : IAsyncDisposable
{
    private readonly ILogger<RabbitMqConsumerService> _logger;
    private readonly RabbitMqSettings _settings;

    private IConnection? _connection;
    private IChannel? _voltageChannel;
    private IChannel? _currentChannel;
    private bool _disposed;

    // ── Events ───────────────────────────────────────────────────────────────
    /// <summary>
    /// Raised for each incoming message. The handler is responsible for
    /// calling AckAsync / NackAsync after processing.
    /// </summary>
    public event Func<MeterMessage, Task>? OnMessageReceived;

    public event EventHandler<ConnectionStatusChangedEventArgs>? OnConnectionStatusChanged;

    // ── Constructor ──────────────────────────────────────────────────────────
    public RabbitMqConsumerService(
        IOptions<MeterConsumerSettings> options,
        ILogger<RabbitMqConsumerService> logger)
    {
        _logger = logger;
        _settings = options.Value.RabbitMq;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Connects to RabbitMQ and starts consuming both queues.
    /// Both queues already exist as durable classic queues — we don't declare them.
    /// We just bind and consume.
    /// </summary>
    public async Task StartAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = _settings.Host,
            Port = _settings.Port,
            UserName = _settings.Username,
            Password = _settings.Password,
            VirtualHost = _settings.VirtualHost,
            // Enables async consumer pipeline (required for AsyncEventingBasicConsumer)
            ConsumerDispatchConcurrency = 2,
            // Auto-recover on network blips
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
        };

        _connection = await factory.CreateConnectionAsync("MeterConsumer", ct).ConfigureAwait(false);

        // Connection-level shutdown event — triggers reconnect logic
        _connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;

        // Start one consumer per queue (separate channels for isolation)
        _voltageChannel = await _connection.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);
        await SetupConsumerAsync(_voltageChannel, _settings.VoltageQueue, MeterMessageType.Voltage, ct).ConfigureAwait(false);

        _currentChannel = await _connection.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);
        await SetupConsumerAsync(_currentChannel, _settings.CurrentQueue, MeterMessageType.Current, ct).ConfigureAwait(false);

        _logger.LogInformation("RabbitMQ connected — consuming queues: {V} and {C}",
            _settings.VoltageQueue, _settings.CurrentQueue);

        OnConnectionStatusChanged?.Invoke(this, new ConnectionStatusChangedEventArgs
        {
            ServiceName = "RabbitMQ",
            IsConnected = true,
            Reason = "Connected and consuming"
        });
    }

    // ── Public ACK / NACK ─────────────────────────────────────────────────────

    /// <summary>
    /// Positive acknowledge — tells RabbitMQ the message was processed safely.
    /// Called by MessageDispatchService after Kafka delivery OR fallback file write.
    /// </summary>
    public async Task AckAsync(MeterMessage message)
    {
        var channel = GetChannelForType(message.Type);
        if (channel is null) return;

        try
        {
            await channel.BasicAckAsync(message.DeliveryTag, multiple: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to ACK message {Id} — broker may re-deliver", message.Id);
        }
    }

    /// <summary>
    /// Negative acknowledge with requeue=false — message goes to dead-letter or is discarded.
    /// Only used for fatal errors (e.g. both Kafka AND fallback file both failed).
    /// </summary>
    public async Task NackAsync(MeterMessage message, bool requeue = false)
    {
        var channel = GetChannelForType(message.Type);
        if (channel is null) return;

        try
        {
            await channel.BasicNackAsync(message.DeliveryTag, multiple: false, requeue: requeue).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to NACK message {Id}", message.Id);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private async Task SetupConsumerAsync(
        IChannel channel,
        string queueName,
        MeterMessageType messageType,
        CancellationToken ct)
    {
        // Prefetch: broker sends at most N unacked messages to this consumer
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _settings.PrefetchCount,
            global: false,
            cancellationToken: ct).ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(channel);

        // Wire the received event
        consumer.ReceivedAsync += async (_, ea) =>
        {
            MeterMessage? message = null;
            try
            {
                var body = Encoding.UTF8.GetString(ea.Body.Span);

                // Deserialize the JSON payload from RabbitMQ
                var raw = JsonSerializer.Deserialize<MeterMessage>(body);

                if (raw is null)
                {
                    _logger.LogWarning("Null message from queue {Queue} — ACKing and discarding", queueName);
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false).ConfigureAwait(false);
                    return;
                }

                // MeterMessage is a sealed class (not a record), so we cannot use 'with'.
                // Construct a new instance, copying all JSON-deserialized fields and
                // injecting the RabbitMQ-specific fields (DeliveryTag, ChannelRef, Type).
                // Type is ALWAYS taken from the source queue — never trusted from the payload.
                message = new MeterMessage
                {
                    Id = raw.Id,
                    MeterId = raw.MeterId,
                    Current = raw.Current,
                    Voltage = raw.Voltage,
                    Timestamp = raw.Timestamp,
                    Type = messageType,        // authoritative: determined by queue name
                    DeliveryTag = ea.DeliveryTag,    // needed for BasicAckAsync
                    ChannelRef = channel            // needed to route ACK to correct channel
                    
                };

                _logger.LogDebug("Received | Queue={Queue} MsgId={Id} MeterId={Meter}",
                    queueName, message.Id, message.MeterId);

                // Raise to MessageDispatchService — it handles ACK after safe delivery
                if (OnMessageReceived is not null)
                    await OnMessageReceived.Invoke(message).ConfigureAwait(false);
            }
            catch (JsonException ex)
            {
                // Invalid JSON — ACK to remove from queue (retrying won't help)
                _logger.LogError(ex, "Invalid JSON in queue {Queue} — discarding", queueName);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error processing message from {Queue}", queueName);
                // Do NOT ACK — let RabbitMQ re-deliver
            }
        };

        // autoAck=false — we manually ACK after safe delivery
        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: ct).ConfigureAwait(false);
    }

    private IChannel? GetChannelForType(MeterMessageType type) =>
        type == MeterMessageType.Voltage ? _voltageChannel : _currentChannel;

    private Task OnConnectionShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        _logger.LogWarning("RabbitMQ connection lost: {Reason}", e.ReplyText);
        OnConnectionStatusChanged?.Invoke(this, new ConnectionStatusChangedEventArgs
        {
            ServiceName = "RabbitMQ",
            IsConnected = false,
            Reason = e.ReplyText
        });
        return Task.CompletedTask;
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_connection is not null)
            _connection.ConnectionShutdownAsync -= OnConnectionShutdownAsync;

        if (_voltageChannel is not null)
            await _voltageChannel.DisposeAsync().ConfigureAwait(false);

        if (_currentChannel is not null)
            await _currentChannel.DisposeAsync().ConfigureAwait(false);

        if (_connection is not null)
            await _connection.DisposeAsync().ConfigureAwait(false);

        _logger.LogInformation("RabbitMqConsumerService disposed");
    }
}