using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace QueueLab.Messaging
{
    public class BroadcastConsumer : BackgroundService
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly RabbitMqSettings _settings;
        private readonly ILogger<BroadcastConsumer> _logger;
        private readonly string _queueName;

        public BroadcastConsumer(ConnectionFactory connectionFactory, IOptions<RabbitMqSettings> settings, ILogger<BroadcastConsumer> logger, string queueName)
        {
            _connectionFactory = connectionFactory;
            _settings = settings.Value;
            _logger = logger;
            _queueName = queueName;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await using var connection = await RabbitMqConnector.ConnectWithRetryAsync(_connectionFactory, _logger, stoppingToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await channel.ExchangeDeclareAsync(
                exchange: _settings.ExchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await channel.QueueDeclareAsync(
                queue: _queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await channel.QueueBindAsync(
                queue: _queueName,
                exchange: _settings.ExchangeName,
                routingKey: string.Empty,
                cancellationToken: stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, args) =>
            {
                var text = Encoding.UTF8.GetString(args.Body.Span);

                _logger.LogInformation("[{Queue}] получил: {Text}", _queueName, text);

                await channel.BasicAckAsync(args.DeliveryTag, multiple: false);
            };

            await channel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            _logger.LogInformation("Broadcast-воркер запущен, слушаю очередь '{Queue}'...", _queueName);

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
