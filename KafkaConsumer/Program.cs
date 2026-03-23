using KafkaConsumer.Core.Interfaces;
using KafkaConsumer.Infrastructure.Configuration;
using KafkaConsumer.Infrastructure.FileWriter;
using KafkaConsumer.Infrastructure.Kafka;
using KafkaConsumer.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        options.ServiceName = "KafkaConsumer";
    })
    .ConfigureServices((context, services) =>
    {
        // 1. Strongly-typed settings
        services.Configure<KafkaConsumerSettings>(
            context.Configuration.GetSection(KafkaConsumerSettings.Section));

        // 2. Shared bounded Channel pipeline
        //    KafkaConsumerService writes → MessagePipeline → FileWriterService reads
        services.AddSingleton<MessagePipeline>();

        // 3. Services
        //    IConsumer<string,string> is built INSIDE KafkaConsumerService only.
        //    FileWriterService has NO Kafka dependency — it only reads the pipeline and writes files.
        services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
        services.AddSingleton<IFileWriterService, FileWriterService>();

        // 4. Background worker (orchestrator)
        services.AddHostedService<KafkaWorker>();
    })
    .Build();

await host.RunAsync();
