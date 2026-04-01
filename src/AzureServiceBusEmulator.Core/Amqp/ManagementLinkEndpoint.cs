using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Handles $management node requests such as cancel-scheduled-message.
/// </summary>
public class ManagementLinkEndpoint : IRequestProcessor
{
    public int Credit => 100;

    private readonly NamespaceContext _context;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;

    public ManagementLinkEndpoint(NamespaceContext context, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _context = context;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Process(RequestContext requestContext)
    {
        var operation = requestContext.Message.ApplicationProperties?["operation"]?.ToString();

        switch (operation)
        {
            case "com.microsoft:cancel-scheduled-message":
                HandleCancelScheduledMessage(requestContext);
                break;

            default:
                ReplyOk(requestContext);
                break;
        }
    }

    private void HandleCancelScheduledMessage(RequestContext requestContext)
    {
        if (_scheduledProcessor is not null && requestContext.Message.Body is Map body)
        {
            if (body.TryGetValue(new Symbol("sequence-numbers"), out var seqNumbers) && seqNumbers is long[] numbers)
            {
                foreach (var seqNo in numbers)
                {
                    _scheduledProcessor.CancelScheduled(seqNo);
                }
            }
        }

        ReplyOk(requestContext);
    }

    private static void ReplyOk(RequestContext requestContext)
    {
        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = requestContext.Message.Properties?.MessageId
            }
        };
        requestContext.Complete(response);
    }
}
