namespace QueueLab.Messaging
{
    public class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "localhost:9092";

        public string Topic { get; set; } = "orders";

        public string GroupId { get; set; } = "queuelab-workers";
    }
}
