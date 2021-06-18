using Kafka.Helper;
using Kafka.Interface;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await Consume();
        }

        static async Task Consume()
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            var serviceProvider = DenpendencyHelper.ServiceProvider;

            var logger = serviceProvider.GetService<ILoggerFactory>().CreateLogger<Program>();
            var kafkaService = serviceProvider.GetService<IKafkaService>();
            var config = serviceProvider.GetService<IConfiguration>();

            logger.LogDebug("Starting application(client)...");

            await kafkaService.SubscribeAsync<object>(new[] { config["Kafka:Topic"] }, async (eventData) =>
            {
                logger.LogInformation($" - Receive: {JsonConvert.SerializeObject(eventData)}");

                await Task.CompletedTask;
            }, cts.Token);

            await Task.CompletedTask;
        }
    }
}
