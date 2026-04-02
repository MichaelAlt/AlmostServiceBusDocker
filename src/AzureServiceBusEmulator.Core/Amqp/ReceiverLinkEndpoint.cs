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

    public ReceiverLinkEndpoint(QueueEntity queue)
    {
        _queue = queue;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // When the client sends drain=true, it wants to stop receiving.
        // Complete the drain immediately so the client can close the link.
        if (flowContext.Link.IsDraining)
        {
            _pumpCts?.Cancel();
            flowContext.Link.CompleteDrain();
            return;
        }

        if (_pumpTask is null || _pumpTask.IsCompleted)
        {
            _pumpCts = new CancellationTokenSource();
            var link = flowContext.Link;

            link.Closed += (_, __) => _pumpCts?.Cancel();
            link.Session.Connection.Closed += (_, __) => _pumpCts?.Cancel();

            _pumpTask = Task.Run(() => MessagePumpAsync(link, _pumpCts.Token));
        }
    }

    private async Task MessagePumpAsync(ListenerLink link, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Use TryDequeueImmediate + short delay instead of blocking DequeueAsync.
                // Blocking DequeueAsync prevents AMQPNetLite from closing the link/connection
                // during graceful shutdown, causing a 30-second timeout.
                var brokered = _queue.TryDequeueImmediate();
                if (brokered is null)
                {
                    await Task.Delay(10, ct);
                    continue;
                }

                try
                {
                    var amqpMessage = ConvertToAmqpMessage(brokered);
                    link.SendMessage(amqpMessage);
                }
                catch
                {
                    // Send failed — link is dead, stop the pump.
                    // Don't re-enqueue: the message is already tracked as pending
                    // and re-enqueueing would create duplicates (R-DUPE).
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch { }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
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

        var message = new Message()
        {
            BodySection = new Data { Binary = brokered.Body ?? [] },
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
