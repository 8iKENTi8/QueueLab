using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace QueueLab.Messaging
{
    public class KafkaOrderProducer : IDisposable
    {
        private readonly KafkaSettings _settings;
        private readonly ILogger<KafkaOrderProducer> _logger;
        private readonly IProducer<string, string> _producer;

        public KafkaOrderProducer(IOptions<KafkaSettings> settings, ILogger<KafkaOrderProducer> logger)
        {
            _settings = settings.Value;
            _logger = logger;

            var config = new ProducerConfig
            {
                BootstrapServers = _settings.BootstrapServers
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishAsync(string orderId, string text, CancellationToken cancellationToken = default)
        {
            var message = new Message<string, string>
            {
                Key = orderId,
                Value = text
            };

            var result = await _producer.ProduceAsync(_settings.Topic, message, cancellationToken);

            _logger.LogInformation(
                "Отправлено в Kafka (partition {Partition}, offset {Offset}): {Text}",
                result.Partition.Value,
                result.Offset.Value,
                text);
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}
