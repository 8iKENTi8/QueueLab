using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace QueueLab.Messaging
{
    public class MessageConsumer : BackgroundService
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly RabbitMqSettings _settings;
        private readonly ILogger<MessageConsumer> _logger;

        public MessageConsumer(ConnectionFactory connectionFactory, IOptions<RabbitMqSettings> settings, ILogger<MessageConsumer> logger)
        {
            _connectionFactory = connectionFactory;
            _settings = settings.Value;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await using var connection = await ConnectWithRetryAsync(stoppingToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await channel.QueueDeclareAsync(
                queue: _settings.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, args) => HandleMessageAsync(channel, args);

            await channel.BasicConsumeAsync(
                queue: _settings.QueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            _logger.LogInformation("Консюмер запущен, слушаю очередь '{Queue}'...", _settings.QueueName);

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        private async Task HandleMessageAsync(IChannel channel, BasicDeliverEventArgs args)
        {
            var text = Encoding.UTF8.GetString(args.Body.Span);

            try
            {
                _logger.LogInformation("Получено из очереди: {Text}", text);

                await Task.Delay(TimeSpan.FromSeconds(1));

                await channel.BasicAckAsync(args.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка обработки сообщения: {Text}", text);
                await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true);
            }
        }

        private async Task<IConnection> ConnectWithRetryAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    return await _connectionFactory.CreateConnectionAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("RabbitMQ недоступен ({Message}). Повтор через 5 секунд...", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
        }
    }
}
