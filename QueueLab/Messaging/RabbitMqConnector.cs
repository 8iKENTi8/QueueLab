using RabbitMQ.Client;

namespace QueueLab.Messaging
{
    public static class RabbitMqConnector
    {
        public static async Task<IConnection> ConnectWithRetryAsync(ConnectionFactory connectionFactory, ILogger logger, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    return await connectionFactory.CreateConnectionAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogWarning("RabbitMQ недоступен ({Message}). Повтор через 5 секунд...", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
        }
    }
}
