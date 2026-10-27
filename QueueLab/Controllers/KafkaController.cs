using Microsoft.AspNetCore.Mvc;
using QueueLab.Messaging;

namespace QueueLab.Controllers
{
    [ApiController]
    [Route("api/kafka")]
    public class KafkaController : ControllerBase
    {
        private readonly KafkaOrderProducer _producer;

        public KafkaController(KafkaOrderProducer producer)
        {
            _producer = producer;
        }

        [HttpPost("orders")]
        public async Task<IActionResult> SendOrder([FromBody] SendOrderRequest request, CancellationToken cancellationToken)
        {
            await _producer.PublishAsync(request.OrderId, request.Text, cancellationToken);

            return Accepted();
        }
    }

    public record SendOrderRequest(string OrderId, string Text);
}