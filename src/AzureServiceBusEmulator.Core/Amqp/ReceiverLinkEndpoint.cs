using System.Reflection;
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
/// Messages are sent pre-settled (ReceiveAndDelete semantics) because
/// AMQPNetLite's ListenerLink generates 4-byte delivery tags, but the
/// Azure SDK expects 16-byte GUIDs for PeekLock settlement. Pre-settling
/// avoids this incompatibility. Messages are auto-completed on delivery.
/// </summary>
public class ReceiverLinkEndpoint : LinkEndpoint
{
    private readonly QueueEntity _queue;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public ReceiverLinkEndpoint(QueueEntity queue)
    {
        _queue = queue;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        if (_pumpTask is null || _pumpTask.IsCompleted)
        {
            // Pre-settle messages so the Azure SDK doesn't need to call Complete().
            // This works around AMQPNetLite's 4-byte delivery tag limitation.
            SetSettleOnSend(flowContext.Link, true);

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
                var brokered = await _queue.DequeueAsync(ct);
                var amqpMessage = ConvertToAmqpMessage(brokered);
                link.SendMessage(amqpMessage);

                // Auto-complete the message since we're pre-settling
                if (brokered.LockToken is not null)
                    _queue.Complete(brokered.LockToken);
            }
        }
        catch (OperationCanceledException) { }
        catch (AmqpException) { }
        catch { }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        // Messages are pre-settled, so dispositions are no-ops.
        // But handle them gracefully in case clients send them anyway.
        var lockToken = GetLockToken(dispositionContext.Message);
        if (lockToken is not null)
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
                _queue.DeadLetter(lockToken, rejected.Error?.Condition?.ToString(), rejected.Error?.Description);
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

    public async Task<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        return await _queue.DequeueAsync(cancellationToken);
    }

    public static Message ConvertToAmqpMessage(BrokeredMessage brokered)
    {
        var lockGuid = Guid.TryParse(brokered.LockToken, out var guid) ? guid : Guid.NewGuid();

        var message = new Message(brokered.Body ?? [])
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
                [new Symbol("x-opt-lock-token")] = lockGuid,
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

    private static void SetSettleOnSend(ListenerLink link, bool value)
    {
        var field = typeof(ListenerLink).GetField("<SettleOnSend>k__BackingField",
            BindingFlags.NonPublic | BindingFlags.Instance);
        field?.SetValue(link, value);
    }

    private static string? GetLockToken(Message message)
    {
        if (message.MessageAnnotations?.Map is not null
            && message.MessageAnnotations.Map.TryGetValue(new Symbol("x-opt-lock-token"), out var token))
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
