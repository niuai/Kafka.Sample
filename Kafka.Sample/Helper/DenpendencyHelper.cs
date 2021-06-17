using Kafka.Sample.Interface;
using Kafka.Sample.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Sample.Helper
{
    public class DenpendencyHelper
    {
        public static ServiceProvider ServiceProvider { get; set; } = ServicePrepare();

        static ServiceProvider ServicePrepare()
        {
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder => builder.AddConsole())
                .AddSingleton<IKafkaService, KafkaService>()
                .BuildServiceProvider();

            return serviceProvider;
        }
    }
}
