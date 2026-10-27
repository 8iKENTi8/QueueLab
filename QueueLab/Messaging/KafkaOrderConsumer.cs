using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace QueueLab.Messaging
{
    public class KafkaOrderConsumer : BackgroundService
    {
        private readonly KafkaSettings _settings;
        private readonly ILogger<KafkaOrderConsumer> _logger;

        public KafkaOrderConsumer(IOptions<KafkaSettings> settings, ILogger<KafkaOrderConsumer> logger)
        {
            _settings = settings.Value;
            _logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
        }

        private void ConsumeLoop(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                GroupId = _settings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_settings.Topic);

            _logger.LogInformation(
                "Kafka-консюмер запущен, группа '{Group}', топик '{Topic}'...",
                _settings.GroupId,
                _settings.Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(stoppingToken);

                    _logger.LogInformation(
                        "[{Group}] Получено (partition {Partition}, offset {Offset}): {Text}",
                        _settings.GroupId,
                        result.Partition.Value,
                        result.Offset.Value,
                        result.Message.Value);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
