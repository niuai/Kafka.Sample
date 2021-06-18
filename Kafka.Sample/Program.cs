using Kafka.Helper;
using Kafka.Interface;
using Kafka.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Kafka.Sample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await Testing();

            await Task.CompletedTask;
        }

        static async Task Testing()
        {
            var serviceProvider = DenpendencyHelper.ServiceProvider;

            var logger = serviceProvider.GetService<ILoggerFactory>().CreateLogger<Program>();
            var kafkaService = serviceProvider.GetService<IKafkaService>();
            var config = serviceProvider.GetService<IConfiguration>();

            logger.LogDebug("Starting application(server)...");

            await kafkaService.PublishAsync(config["Kafka:Topic"], new TestEventData { Id = 1, Message = "hello" });

            await Task.CompletedTask;
        }
    }
}
