using MeterConsumer.Application.Services;
using MeterConsumer.Core.Events;
using MeterConsumer.Core.Interfaces;
using MeterConsumer.Infrastructure.Configuration;
using MeterConsumer.Infrastructure.RabbitMq;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MeterConsumer.Worker;

/// <summary>
/// Windows Service entry point.
/// Orchestrates: startup, consumer wiring, replay loop, health logging, graceful shutdown.
///
/// SHUTDOWN SEQUENCE:
/// ─────────────────────────────────────────────────────────────
///  1. CancellationToken cancelled (SC STOP or Windows shutdown)
///  2. RabbitMQ consumers stop accepting new messages (StopConsuming via Dispose)
///  3. In-flight messages complete processing (Kafka flush / file write)
///  4. FallbackReplayService loop exits cleanly
///  5. All services disposed (events unsubscribed, channels closed)
/// ─────────────────────────────────────────────────────────────
/// </summary>
public sealed class MeterWorker : BackgroundService
{
    private readonly ILogger<MeterWorker> _logger;
    private readonly RabbitMqConsumerService _rabbitConsumer;
    private readonly MessageDispatchService _dispatcher;
    private readonly FallbackReplayService _replayService;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly ProcessingSettings _processingSettings;
    private readonly WorkerSettings _workerSettings;

    // Health log timer
    private Timer? _healthTimer;
    private long _totalReceived = 0;
    private long _totalDelivered = 0;
    private long _totalFailed = 0;

    public MeterWorker(
        RabbitMqConsumerService rabbitConsumer,
        MessageDispatchService dispatcher,
        FallbackReplayService replayService,
        IKafkaProducer kafkaProducer,
        IOptions<MeterConsumerSettings> options,
        ILogger<MeterWorker> logger)
    {
        _rabbitConsumer = rabbitConsumer;
        _dispatcher = dispatcher;
        _replayService = replayService;
        _kafkaProducer = kafkaProducer;
        _processingSettings = options.Value.Processing;
        _workerSettings = new WorkerSettings(); // health interval
        _logger = logger;
    }

    // ── BackgroundService ─────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MeterConsumer service starting...");

        try
        {
            // Step 1: Ensure Kafka topics exist (create if missing)
            await _kafkaProducer.EnsureTopicsExistAsync(stoppingToken).ConfigureAwait(false);

            // Step 2: Subscribe to events
            WireEvents();

            // Step 3: Start health logging timer (every 30 seconds)
            _healthTimer = new Timer(
                LogHealth,
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30));

            // Step 4: Connect RabbitMQ and start consuming
            await _rabbitConsumer.StartAsync(stoppingToken).ConfigureAwait(false);

            _logger.LogInformation("MeterConsumer service RUNNING — consuming meter_voltage and meter_current");

            // Step 5: Run fallback replay loop (concurrent with message processing)
            var replayTask = _replayService.RunAsync(stoppingToken);

            // Step 6: Wait until service stop is requested
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);

            // Wait for replay loop to finish cleanly
            await replayTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("MeterConsumer service stopping (shutdown requested)");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "MeterConsumer service encountered a fatal error");
            throw;  // Let the host handle restart / event log
        }
        finally
        {
            await CleanupAsync().ConfigureAwait(false);
        }
    }

    // ── Event wiring ──────────────────────────────────────────────────────────

    private void WireEvents()
    {
        // Dispatcher events
        _dispatcher.OnMessageReceived += OnMessageReceivedHandler;
        _dispatcher.OnMessageProcessed += OnMessageProcessedHandler;
        _dispatcher.OnMessageFailed += OnMessageFailedHandler;

        // Kafka circuit breaker status
        _kafkaProducer.OnConnectionStatusChanged += OnConnectionStatusChangedHandler;

        // RabbitMQ connection status
        _rabbitConsumer.OnConnectionStatusChanged += OnConnectionStatusChangedHandler;

        // Wire RabbitMQ → Dispatcher
        _rabbitConsumer.OnMessageReceived += _dispatcher.HandleIncomingAsync;
    }

    private void UnwireEvents()
    {
        _dispatcher.OnMessageReceived -= OnMessageReceivedHandler;
        _dispatcher.OnMessageProcessed -= OnMessageProcessedHandler;
        _dispatcher.OnMessageFailed -= OnMessageFailedHandler;
        _kafkaProducer.OnConnectionStatusChanged -= OnConnectionStatusChangedHandler;
        _rabbitConsumer.OnConnectionStatusChanged -= OnConnectionStatusChangedHandler;
        _rabbitConsumer.OnMessageReceived -= _dispatcher.HandleIncomingAsync;
    }

    // ── Event handlers ────────────────────────────────────────────────────────

    private void OnMessageReceivedHandler(object? sender, MessageReceivedEventArgs e)
    {
        Interlocked.Increment(ref _totalReceived);
        _logger.LogDebug("Received | MsgId={Id} Queue={Queue} Thread={Thread}",
            e.MessageId, e.QueueName, Environment.CurrentManagedThreadId);
    }

    private void OnMessageProcessedHandler(object? sender, MessageProcessedEventArgs e)
    {
        Interlocked.Increment(ref _totalDelivered);
        _logger.LogInformation(
            "Delivered | MsgId={Id} Topic={Topic} Partition={P} Offset={O} Thread={Thread}",
            e.MessageId, e.KafkaTopic, e.Partition, e.Offset, Environment.CurrentManagedThreadId);
    }

    private void OnMessageFailedHandler(object? sender, MessageFailedEventArgs e)
    {
        Interlocked.Increment(ref _totalFailed);

        if (e.SavedToFallback)
            _logger.LogWarning("Fallback | MsgId={Id} Reason={Reason}", e.MessageId, e.Reason);
        else
            _logger.LogError(e.Exception, "FAILED | MsgId={Id} Reason={Reason}", e.MessageId, e.Reason);
    }

    private void OnConnectionStatusChangedHandler(object? sender, ConnectionStatusChangedEventArgs e)
    {
        if (e.IsConnected)
            _logger.LogInformation("{Service} CONNECTED: {Reason}", e.ServiceName, e.Reason);
        else
            _logger.LogWarning("{Service} DISCONNECTED: {Reason}", e.ServiceName, e.Reason);
    }

    // ── Health logging ────────────────────────────────────────────────────────

    private void LogHealth(object? state)
    {
        _logger.LogInformation(
            "[HEALTH] Received={R} Delivered={D} Failed={F} KafkaUp={K} FallbackPending={FP}",
            Interlocked.Read(ref _totalReceived),
            Interlocked.Read(ref _totalDelivered),
            Interlocked.Read(ref _totalFailed),
            _kafkaProducer.IsAvailable,
            _replayService is not null);  // TODO: expose pending count via interface
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    private async Task CleanupAsync()
    {
        _logger.LogInformation("MeterConsumer cleanup starting...");

        UnwireEvents();

        _healthTimer?.Dispose();

        await _rabbitConsumer.DisposeAsync().ConfigureAwait(false);
        await _dispatcher.DisposeAsync().ConfigureAwait(false);
        await _replayService.DisposeAsync().ConfigureAwait(false);
        await _kafkaProducer.DisposeAsync().ConfigureAwait(false);

        _logger.LogInformation("MeterConsumer cleanup complete");
    }

    /// <summary>Internal helper for health timer interval.</summary>
    private sealed class WorkerSettings
    {
        public int HealthLogIntervalSeconds { get; set; } = 30;
    }
}
