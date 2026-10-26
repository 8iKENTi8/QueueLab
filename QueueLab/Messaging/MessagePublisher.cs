using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace QueueLab.Messaging
{
    public class MessagePublisher : IAsyncDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly RabbitMqSettings _settings;
        private readonly ILogger<MessagePublisher> _logger;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private IConnection? _connection;

        public MessagePublisher(ConnectionFactory connectionFactory, IOptions<RabbitMqSettings> settings, ILogger<MessagePublisher> logger)
        {
            _connectionFactory = connectionFactory;
            _settings = settings.Value;
            _logger = logger;
        }

        public async Task PublishAsync(string text, CancellationToken cancellationToken = default)
        {
            var connection = await GetConnectionAsync(cancellationToken);

            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: _settings.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: cancellationToken);

            var properties = new BasicProperties
            {
                Persistent = true
            };

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: _settings.QueueName,
                mandatory: false,
                basicProperties: properties,
                body: Encoding.UTF8.GetBytes(text),
                cancellationToken: cancellationToken);

            _logger.LogInformation("Отправлено в очередь '{Queue}': {Text}", _settings.QueueName, text);
        }

        public async Task PublishBroadcastAsync(string text, CancellationToken cancellationToken = default)
        {
            var connection = await GetConnectionAsync(cancellationToken);

            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await channel.ExchangeDeclareAsync(
                exchange: _settings.ExchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            var properties = new BasicProperties
            {
                Persistent = true
            };

            await channel.BasicPublishAsync(
                exchange: _settings.ExchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: properties,
                body: Encoding.UTF8.GetBytes(text),
                cancellationToken: cancellationToken);

            _logger.LogInformation("Broadcast в exchange '{Exchange}': {Text}", _settings.ExchangeName, text);
        }

        private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            await _connectionLock.WaitAsync(cancellationToken);
            try
            {
                _connection ??= await _connectionFactory.CreateConnectionAsync(cancellationToken);
                return _connection;
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connectionLock.Dispose();
        }
    }
}
