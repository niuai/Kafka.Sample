using Confluent.Kafka;
using Kafka.Interface;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Service
{
    public class KafkaService : IKafkaService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;

        public KafkaService(IConfiguration configuration, ILogger<KafkaService> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task PublishAsync<TMessage>(string topicName, TMessage message) where TMessage : class
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"]
            };
            using var producer = new ProducerBuilder<string, string>(config).Build();
            await producer.ProduceAsync(topicName, new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = JsonConvert.SerializeObject(message)
            });
        }

        public async Task SubscribeAsync<TMessage>(IEnumerable<string> topics, Action<TMessage> messageFunc, CancellationToken cancellationToken) where TMessage : class
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                GroupId = _configuration["Kafka:ConsumerGroupId"],
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) =>
                {
                    _logger.LogError($"Error: {e.Reason}");
                })
                .SetStatisticsHandler((_, json) =>
                {
                    _logger.LogDebug($" - {DateTime.Now:yyyy-MM-dd HH:mm:ss} > listening kafka...");
                })
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogTrace($" - Assign kafka partitions: {string.Join(", ", partitions)}");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogTrace($" - Recovery kafka partitions: {string.Join(", ", partitions)}");
                })
                .Build();

            consumer.Subscribe(topics);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        _logger.LogTrace($"Consumed message '{consumeResult.Message?.Value}' at: '{consumeResult?.TopicPartitionOffset}'.");

                        if (consumeResult.IsPartitionEOF)
                        {
                            _logger.LogTrace($" - {DateTime.Now:yyyy-MM-dd HH:mm:ss} kafka has been read to end: {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            continue;
                        }
                        TMessage messageResult = null;
                        try
                        {
                            messageResult = JsonConvert.DeserializeObject<TMessage>(consumeResult.Message.Value);
                        }
                        catch (Exception ex)
                        {
                            var errorMessage = $" - {DateTime.Now:yyyy-MM-dd HH:mm:ss}[Exception: DeserializeObject error, value: {consumeResult.Message.Value}]: {ex.StackTrace?.ToString()}";
                            _logger.LogError(errorMessage);
                            messageResult = null;
                        }
                        if (messageResult != null/* && consumeResult.Offset % commitPeriod == 0*/)
                        {
                            messageFunc(messageResult);
                            try
                            {
                                consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                _logger.LogError(e, e.Message);
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError(e, $"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Closing consumer.");
                consumer.Close();
            }

            await Task.CompletedTask;
        }
    }
}
