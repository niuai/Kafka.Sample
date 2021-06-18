using Kafka.Interface;
using Kafka.Service;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.IO;

namespace Kafka.Helper
{
    public class DenpendencyHelper
    {
        public static ServiceProvider ServiceProvider { get; set; } = ServicePrepare();

        static ServiceProvider ServicePrepare()
        {
            var config = Config();
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder => builder.AddConsole().AddFilter(level => level >= LogLevel.Debug))
                .AddSingleton<IKafkaService, KafkaService>()
                .AddSingleton(config)
                .BuildServiceProvider();

            return serviceProvider;
        }

        static IConfiguration Config()
        {
            var builder = new ConfigurationBuilder();

            builder.SetBasePath(Directory.GetCurrentDirectory());
            builder.AddJsonFile("appsettings.json", true, true);

            return builder.Build();
        }
    }
}
