namespace QueueLab.Messaging
{
    public class RabbitMqSettings
    {
        public string HostName { get; set; } = "localhost";

        public int Port { get; set; } = 5672;

        public string UserName { get; set; } = "guest";

        public string Password { get; set; } = "guest";

        public string QueueName { get; set; } = "demo-queue";

        public string ExchangeName { get; set; } = "lab-exchange";

        public string BroadcastQueueA { get; set; } = "lab-queue-a";

        public string BroadcastQueueB { get; set; } = "lab-queue-b";
    }
}
