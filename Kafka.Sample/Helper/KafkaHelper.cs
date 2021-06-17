using Confluent.Kafka;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Sample.Helper
{
    public class KafkaHelper
    {
        public static async Task PublishMessage(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList topicName");
                // 127.0.0.1:9092 helloTopic
                return;
            }

            var brokerList = args.First();
            var topicName = args.Last();

            var config = new ProducerConfig { BootstrapServers = brokerList };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            Console.WriteLine("\n-----------------------------------------------------------------------");
            Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
            Console.WriteLine("-----------------------------------------------------------------------");
            Console.WriteLine("To create a kafka message with UTF-8 encoded key and value:");
            Console.WriteLine("> key value<Enter>");
            Console.WriteLine("To create a kafka message with a null key and UTF-8 encoded value:");
            Console.WriteLine("> value<enter>");
            Console.WriteLine("Ctrl-C to quit.\n");

            var cancelled = false;

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancelled = true;
            };

            while (!cancelled)
            {
                Console.Write("> ");

                var text = string.Empty;

                try
                {
                    text = Console.ReadLine();
                }
                catch (IOException)
                {
                    break;
                }

                if (string.IsNullOrWhiteSpace(text))
                {
                    break;
                }

                var key = string.Empty;
                var val = text;

                var index = text.IndexOf(" ");
                if (index != -1)
                {
                    key = text.Substring(0, index);
                    val = text.Substring(index + 1);
                }

                try
                {
                    var deliveryResult = await producer.ProduceAsync(topicName, new Message<string, string>
                    {
                        Key = key,
                        Value = val
                    });

                    Console.WriteLine($"delivered to: {deliveryResult.TopicPartitionOffset}");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }

        public static async Task Consume(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList topicName");
                // 127.0.0.1:9092 helloTopic
                return;
            }

            var brokerList = args.First();
            var topicName = args.Last();

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            const int commitPeriod = 5;

            using var consumer = new ConsumerBuilder<Ignore, string>(config)
                                 .SetErrorHandler((_, e) =>
                                 {
                                     Console.WriteLine($"Error: {e.Reason}");
                                 })
                                 .SetStatisticsHandler((_, json) =>
                                 {
                                     Console.WriteLine($" - {DateTime.Now:yyyy-MM-dd HH:mm:ss} > monitoring..");
                                     //Console.WriteLine($"Statistics: {json}");
                                 })
                                 .SetPartitionsAssignedHandler((c, partitions) =>
                                 {
                                     Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                                 })
                                 .SetPartitionsRevokedHandler((c, partitions) =>
                                 {
                                     Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                                 })
                                 .Build();
            consumer.Subscribe(topicName);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                        if (consumeResult.Offset % commitPeriod == 0)
                        {
                            try
                            {
                                consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Commit error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }

            await Task.CompletedTask;
        }
    }
}
