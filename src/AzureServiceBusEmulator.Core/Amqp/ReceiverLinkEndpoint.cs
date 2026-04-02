using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Server-side endpoint for sending messages to clients.
/// When a client has a receiver link, the server has a sender endpoint.
///
/// The Azure SDK grants credit upfront and expects messages to be pushed
/// as they arrive. We start a background pump that continuously dequeues
/// from the queue and sends to the client while credit is available.
/// </summary>
public class ReceiverLinkEndpoint : LinkEndpoint
{
    private readonly QueueEntity _queue;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;
    private ListenerLink? _link;

    public ReceiverLinkEndpoint(QueueEntity queue)
    {
        _queue = queue;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // Start the message pump on first flow if not already running.
        // The pump continuously dequeues and sends while the link has credit.
        if (_pumpTask is null || _pumpTask.IsCompleted)
        {
            _link = flowContext.Link;
            _pumpCts = new CancellationTokenSource();
            _pumpTask = Task.Run(() => MessagePumpAsync(flowContext.Link, _pumpCts.Token));
        }
    }

    private async Task MessagePumpAsync(ListenerLink link, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && !link.IsClosed)
            {
                // Wait for a message (blocks until one is available)
                var brokered = await _queue.DequeueAsync(ct);

                var amqpMessage = ConvertToAmqpMessage(brokered);
                link.SendMessage(amqpMessage);
            }
        }
        catch (OperationCanceledException) { }
        catch (AmqpException) { } // Link closed
        catch { } // Other errors — pump stops
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        var lockToken = GetLockToken(dispositionContext.Message);

        if (lockToken is null)
        {
            dispositionContext.Complete();
            return;
        }

        SettleMessage(lockToken, dispositionContext.DeliveryState);
        dispositionContext.Complete();
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        _pumpCts?.Cancel();
        _pumpCts?.Dispose();
        _pumpCts = null;
        base.OnLinkClosed(link, error);
    }

    /// <summary>
    /// Settles a message by lock token based on the AMQP delivery state.
    /// </summary>
    public void SettleMessage(string lockToken, DeliveryState deliveryState)
    {
        switch (deliveryState)
        {
            case Accepted:
                _queue.Complete(lockToken);
                break;

            case Released:
                _queue.Abandon(lockToken);
                break;

            case Rejected rejected:
                var reason = rejected.Error?.Condition?.ToString();
                var description = rejected.Error?.Description;
                _queue.DeadLetter(lockToken, reason, description);
                break;

            case Modified modified:
                if (modified.UndeliverableHere == true)
                    _queue.DeadLetter(lockToken, "Undeliverable", "Message marked as undeliverable.");
                else
                    _queue.Abandon(lockToken);
                break;

            default:
                _queue.Complete(lockToken);
                break;
        }
    }

    /// <summary>
    /// Dequeues a single message from the queue. Exposed for testing.
    /// </summary>
    public async Task<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        return await _queue.DequeueAsync(cancellationToken);
    }

    /// <summary>
    /// Converts a <see cref="BrokeredMessage"/> to an AMQP <see cref="Message"/>.
    /// </summary>
    public static Message ConvertToAmqpMessage(BrokeredMessage brokered)
    {
        var message = new Message(new Data { Binary = brokered.Body })
        {
            Properties = new Properties
            {
                MessageId = brokered.MessageId,
                CorrelationId = brokered.CorrelationId,
                ContentType = brokered.ContentType,
                Subject = brokered.Subject,
                ReplyTo = brokered.ReplyTo,
                To = brokered.To,
                GroupId = brokered.SessionId,
                ReplyToGroupId = brokered.ReplyToSessionId
            },
            Header = new Header
            {
                DeliveryCount = (uint)brokered.DeliveryCount
            },
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-sequence-number")] = brokered.SequenceNumber,
                [new Symbol("x-opt-enqueued-time")] = brokered.EnqueuedTimeUtc.UtcDateTime,
                [new Symbol("x-opt-lock-token")] = Guid.TryParse(brokered.LockToken, out var guid) ? guid : Guid.NewGuid(),
                [new Symbol("x-opt-locked-until")] = DateTimeOffset.UtcNow.Add(LockDuration).UtcDateTime
            }
        };

        if (brokered.ApplicationProperties.Count > 0)
        {
            message.ApplicationProperties = new ApplicationProperties();
            foreach (var kvp in brokered.ApplicationProperties)
            {
                message.ApplicationProperties[kvp.Key] = kvp.Value;
            }
        }

        return message;
    }

    private static readonly TimeSpan LockDuration = TimeSpan.FromSeconds(30);

    private static string? GetLockToken(Message message)
    {
        if (message.MessageAnnotations?.Map is null)
            return null;

        if (message.MessageAnnotations.Map.TryGetValue(new Symbol("x-opt-lock-token"), out var token))
        {
            return token switch
            {
                Guid g => g.ToString(),
                string s => s,
                _ => token?.ToString()
            };
        }

        return null;
    }
}
