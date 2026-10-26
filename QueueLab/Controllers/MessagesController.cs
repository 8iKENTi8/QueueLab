using Microsoft.AspNetCore.Mvc;
using QueueLab.Messaging;

namespace QueueLab.Controllers
{
    [ApiController]
    [Route("api/messages")]
    public class MessagesController : ControllerBase
    {
        private readonly MessagePublisher _publisher;

        public MessagesController(MessagePublisher publisher)
        {
            _publisher = publisher;
        }

        [HttpPost]
        public async Task<IActionResult> Send([FromBody] SendMessageRequest request, CancellationToken cancellationToken)
        {
            await _publisher.PublishAsync(request.Text, cancellationToken);

            return Accepted();
        }

        [HttpPost("broadcast")]
        public async Task<IActionResult> Broadcast([FromBody] SendMessageRequest request, CancellationToken cancellationToken)
        {
            await _publisher.PublishBroadcastAsync(request.Text, cancellationToken);

            return Accepted();
        }
    }

    public record SendMessageRequest(string Text);
}
