using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Server-side endpoint for receiving messages from clients.
/// When a client has a sender link, the server has a receiver endpoint.
/// </summary>
public class SenderLinkEndpoint : LinkEndpoint
{
    private readonly NamespaceContext _context;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly string _address;

    public SenderLinkEndpoint(NamespaceContext context, string address, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _context = context;
        _address = address;
        _scheduledProcessor = scheduledProcessor;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        try
        {
            var brokeredMessage = ConvertToBrokeredMessage(messageContext.Message);
            RouteMessage(_address, brokeredMessage);
            messageContext.Complete();
        }
        catch
        {
            messageContext.Complete(new global::Amqp.Framing.Error(new Symbol("amqp:internal-error"))
            {
                Description = "Failed to process message"
            });
        }
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // No-op: sender link endpoints do not need to handle flow.
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }

    /// <summary>
    /// Converts an AMQP message to a <see cref="BrokeredMessage"/>.
    /// Exposed as public for testing.
    /// </summary>
    public static BrokeredMessage ConvertToBrokeredMessage(Message amqpMessage)
    {
        var brokered = new BrokeredMessage();

        // Extract body
        if (amqpMessage.Body is byte[] bodyBytes)
        {
            brokered.Body = bodyBytes;
        }
        else if (amqpMessage.Body is Data data)
        {
            brokered.Body = data.Binary;
        }

        // Extract standard properties
        if (amqpMessage.Properties is not null)
        {
            var props = amqpMessage.Properties;

            if (props.MessageId is not null)
                brokered.MessageId = props.MessageId.ToString()!;
            if (props.CorrelationId is not null)
                brokered.CorrelationId = props.CorrelationId.ToString();
            if (props.ContentType is not null)
                brokered.ContentType = props.ContentType;
            if (props.Subject is not null)
                brokered.Subject = props.Subject;
            if (props.ReplyTo is not null)
                brokered.ReplyTo = props.ReplyTo;
            if (props.To is not null)
                brokered.To = props.To;
            if (props.GroupId is not null)
                brokered.SessionId = props.GroupId;
            if (props.ReplyToGroupId is not null)
                brokered.ReplyToSessionId = props.ReplyToGroupId;
        }

        // Extract application properties
        if (amqpMessage.ApplicationProperties?.Map is not null)
        {
            foreach (var kvp in amqpMessage.ApplicationProperties.Map)
            {
                brokered.ApplicationProperties[kvp.Key.ToString()!] = kvp.Value;
            }
        }

        // Extract message annotations
        if (amqpMessage.MessageAnnotations?.Map is not null)
        {
            var annotations = amqpMessage.MessageAnnotations.Map;

            if (annotations.TryGetValue(new Symbol("x-opt-scheduled-enqueue-time"), out var scheduledTime))
            {
                brokered.ScheduledEnqueueTimeUtc = scheduledTime switch
                {
                    DateTimeOffset dto => dto,
                    DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
                    _ => null
                };
            }

            if (annotations.TryGetValue(new Symbol("x-opt-partition-key"), out var partitionKey))
            {
                brokered.PartitionKey = partitionKey?.ToString();
            }
        }

        // Extract TTL from header
        if (amqpMessage.Header?.Ttl > 0)
        {
            brokered.TimeToLive = TimeSpan.FromMilliseconds(amqpMessage.Header.Ttl);
        }

        return brokered;
    }

    /// <summary>
    /// Routes a brokered message to the appropriate queue or topic.
    /// Exposed as public for testing.
    /// </summary>
    public void RouteMessage(string address, BrokeredMessage message)
    {
        message.SequenceNumber = _context.NextSequenceNumber();
        message.EnqueuedTimeUtc = DateTimeOffset.UtcNow;

        // Check if the message should be scheduled
        if (message.ScheduledEnqueueTimeUtc.HasValue
            && message.ScheduledEnqueueTimeUtc.Value > DateTimeOffset.UtcNow
            && _scheduledProcessor is not null)
        {
            _scheduledProcessor.Schedule(address, message);
            return;
        }

        var (queue, topic) = _context.ResolveSendTarget(address);

        if (queue is not null)
        {
            queue.Enqueue(message);
        }
        else if (topic is not null)
        {
            topic.Publish(message);
        }
        else
        {
            throw new InvalidOperationException($"No queue or topic found for address '{address}'.");
        }
    }
}
