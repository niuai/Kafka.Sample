using Confluent.Kafka;
using Kafka.Sample.Helper;
using Kafka.Sample.Interface;
using Kafka.Sample.Models;
using Kafka.Sample.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Sample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //await Testing();
            await Consume();
        }

        static async Task Testing()
        {
            var serviceProvider = DenpendencyHelper.ServiceProvider;

            var logger = serviceProvider.GetService<ILoggerFactory>().CreateLogger<Program>();
            var kafkaService = serviceProvider.GetService<IKafkaService>();

            logger.LogDebug("Starting application");

            await kafkaService.PublishAsync("test", new TestEventData { Id = 1, Message = "hello" });

            // do the actual work here


            logger.LogDebug("All done!");

            await Task.CompletedTask;
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

            logger.LogDebug("Starting application");

            await kafkaService.SubscribeAsync<TestEventData>(new[] { "test" }, async (eventData) =>
            {
                Console.WriteLine($" - {eventData.EventTime:yyyy-MM-dd HH:mm:ss} 【{eventData.TopicName}】- > 已处理");
                await Task.CompletedTask;
            }, cts.Token);


            logger.LogDebug("All done!");

            await Task.CompletedTask;
        }
    }
}
