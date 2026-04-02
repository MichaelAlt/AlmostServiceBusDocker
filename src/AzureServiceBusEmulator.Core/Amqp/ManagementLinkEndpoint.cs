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

            case "com.microsoft:schedule-message":
                HandleScheduleMessage(requestContext);
                break;

            case "com.microsoft:renew-lock":
                HandleRenewLock(requestContext);
                break;

            default:
                ReplyOk(requestContext);
                break;
        }
    }

    private void HandleScheduleMessage(RequestContext requestContext)
    {
        var sequenceNumbers = new List<long>();

        if (_scheduledProcessor is not null && requestContext.Message.Body is Map scheduleBody)
        {
            var entityName = requestContext.Message.ApplicationProperties?["associated-link-name"]?.ToString();

            if (scheduleBody.TryGetValue(new Symbol("messages"), out var messagesObj) && messagesObj is List messagesList)
            {
                foreach (var item in messagesList)
                {
                    if (item is not Map msgMap) continue;

                    // Extract the inner AMQP message
                    Message? innerMessage = null;
                    if (msgMap.TryGetValue(new Symbol("message"), out var msgBytes) && msgBytes is byte[] rawMessage)
                    {
                        innerMessage = Message.Decode(new ByteBuffer(rawMessage, 0, rawMessage.Length, rawMessage.Length));
                    }

                    // Extract the message-id
                    string? messageId = null;
                    if (msgMap.TryGetValue(new Symbol("message-id"), out var mid))
                        messageId = mid?.ToString();

                    if (innerMessage is not null)
                    {
                        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(innerMessage);
                        if (messageId is not null)
                            brokered.MessageId = messageId;

                        // Resolve the entity to schedule on
                        var address = entityName?.TrimStart('/') ?? string.Empty;
                        var seqNo = _scheduledProcessor.Schedule(address, brokered);
                        sequenceNumbers.Add(seqNo);
                    }
                }
            }
        }

        // Return the sequence numbers as the response
        var responseBody = new Map
        {
            { new Symbol("sequence-numbers"), sequenceNumbers.ToArray() }
        };
        var response = new Message(responseBody)
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

    private void HandleRenewLock(RequestContext requestContext)
    {
        var expirations = new List<DateTime>();

        if (requestContext.Message.Body is Map renewBody
            && TryGetMapValue(renewBody, "lock-tokens", out var tokensObj)
            && tokensObj is Guid[] lockTokenGuids)
        {
            // Try to find the queue. The entity name may be in the associated-link-name
            // or we scan all queues for the lock token.
            foreach (var lockGuid in lockTokenGuids)
            {
                var lockToken = lockGuid.ToString();
                DateTimeOffset? newExpiry = null;

                // Scan all queues in the namespace for the lock token
                foreach (var queue in _context.GetQueues())
                {
                    newExpiry = queue.RenewLock(lockToken);
                    if (newExpiry.HasValue) break;

                    // Also check the queue's DLQ
                    newExpiry = queue.DeadLetterQueue.RenewLock(lockToken);
                    if (newExpiry.HasValue) break;
                }

                // Also check subscription queues
                if (!newExpiry.HasValue)
                {
                    foreach (var topic in _context.GetTopics())
                    {
                        foreach (var sub in topic.GetSubscriptions())
                        {
                            newExpiry = sub.Queue.RenewLock(lockToken);
                            if (newExpiry.HasValue) break;
                        }
                        if (newExpiry.HasValue) break;
                    }
                }

                expirations.Add(newExpiry?.UtcDateTime ?? DateTime.UtcNow.AddMinutes(5));
            }
        }

        var responseBody = new Map
        {
            { "expirations", expirations.ToArray() }
        };
        var response = new Message(responseBody)
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

    /// <summary>
    /// Looks up a key in an AMQP Map, trying both Symbol and String key types.
    /// The Azure SDK's Microsoft.Azure.Amqp library encodes map keys as strings,
    /// while AMQPNetLite uses Symbols.
    /// </summary>
    private static bool TryGetMapValue(Map map, string key, out object? value)
    {
        if (map.TryGetValue(new Symbol(key), out value))
            return true;
        if (map.TryGetValue(key, out value))
            return true;
        value = null;
        return false;
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
