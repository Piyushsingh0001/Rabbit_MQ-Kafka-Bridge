using MeterConsumer.Application.Services;
using MeterConsumer.Core.Interfaces;
using MeterConsumer.Infrastructure.Configuration;
using MeterConsumer.Infrastructure.Fallback;
using MeterConsumer.Infrastructure.Kafka;
using MeterConsumer.Infrastructure.RabbitMq;
using MeterConsumer.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// ─────────────────────────────────────────────────────────────────────────────
// HOST BUILDER
// ─────────────────────────────────────────────────────────────────────────────
var host = Host.CreateDefaultBuilder(args)
    // ── Enables running as a Windows Service (sc create / Start-Service)
    // ── Falls back to normal console app when run interactively (dev/debug)
    .UseWindowsService(options =>
    {
        options.ServiceName = "MeterConsumer";
    })
    .ConfigureServices((context, services) =>
    {
        // ── 1. Strongly-typed settings (bound from appsettings.json)
        services.Configure<MeterConsumerSettings>(
            context.Configuration.GetSection(MeterConsumerSettings.Section));

        // ── 2. Infrastructure — Kafka producer (IKafkaProducer)
        //       Registered as Singleton: one producer for the service lifetime
        //       Confluent.Kafka producer is thread-safe and designed for reuse
        services.AddSingleton<IKafkaProducer, KafkaProducerService>();

        // ── 3. Infrastructure — Local fallback JSONL store (IFallbackStore)
        //       Singleton: one file handle + semaphore shared across the pipeline
        services.AddSingleton<IFallbackStore, LocalFallbackStore>();

        // ── 4. Infrastructure — RabbitMQ consumer
        //       Singleton: maintains the AMQP connection and two channels
        services.AddSingleton<RabbitMqConsumerService>();

        // ── 5. Application — Message dispatcher
        //       Singleton: owns the live-message Channel<T> buffer
        services.AddSingleton<MessageDispatchService>();

        // ── 6. Application — Fallback replay background loop
        //       Singleton: runs a periodic Task inside ExecuteAsync
        services.AddSingleton<FallbackReplayService>();

        // ── 7. Worker — the BackgroundService that ties everything together
        services.AddHostedService<MeterWorker>();
    })
    .Build();

await host.RunAsync();
