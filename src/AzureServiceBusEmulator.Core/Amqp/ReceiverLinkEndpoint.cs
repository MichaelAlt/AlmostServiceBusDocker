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
    private int _credit;

    public ReceiverLinkEndpoint(QueueEntity queue)
    {
        _queue = queue;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [FLOW] queue='{_queue.Name}' credit={flowContext.Messages} drain={flowContext.Link.IsDraining}");

        // When the client sends drain=true, it wants to stop receiving.
        // Complete the drain immediately so the client can close the link.
        if (flowContext.Link.IsDraining)
        {
            _pumpCts?.Cancel();
            flowContext.Link.CompleteDrain();
            return;
        }

        // Track credit from the client. flowContext.Messages is the total
        // credit the client is granting (not incremental).
        Interlocked.Exchange(ref _credit, flowContext.Messages);

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
                // Respect AMQP flow control: only send messages when the client
                // has granted credit. Without this check, we'd dequeue messages
                // the client didn't ask for, causing them to be Released/abandoned.
                if (Volatile.Read(ref _credit) <= 0)
                {
                    await Task.Delay(10, ct);
                    continue;
                }

                // Use TryDequeueImmediate + short delay instead of blocking DequeueAsync.
                // Blocking DequeueAsync prevents AMQPNetLite from closing the link/connection
                // during graceful shutdown, causing a 30-second timeout.
                var brokered = _queue.TryDequeueImmediate();
                if (brokered is null)
                {
                    await Task.Delay(10, ct);
                    continue;
                }

                // Consume one unit of credit before sending
                Interlocked.Decrement(ref _credit);

                try
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [PUMP] {brokered.MessageId} → '{_queue.Name}'");
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
        var stateInfo = dispositionContext.DeliveryState switch
        {
            Rejected r => $"Rejected: {r.Error?.Condition} {r.Error?.Description}",
            Modified m => $"Modified: undeliverable={m.UndeliverableHere} failed={m.DeliveryFailed}",
            _ => dispositionContext.DeliveryState?.GetType().Name ?? "null"
        };
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [DISP] lock={lockToken} state={stateInfo} queue='{_queue.Name}'");

        try
        {
            if (lockToken is not null && dispositionContext.DeliveryState is not null)
                SettleMessage(lockToken, dispositionContext.DeliveryState);

            dispositionContext.Complete();
        }
        catch (MessageLockLostException)
        {
            // The message lock has expired. The message has been re-enqueued for redelivery.
            // Ideally we'd send a Rejected disposition with com.microsoft:message-lock-lost,
            // but AMQPNetLite's DispositionContext.Complete(Error) detaches the link entirely.
            // Instead, we just accept the disposition — the message is already re-enqueued
            // and will be redelivered. The client won't get a lock-lost error, but will
            // see the message again with an incremented DeliveryCount.
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [DISP] lock={lockToken} LOCK EXPIRED (re-enqueued) queue='{_queue.Name}'");
            dispositionContext.Complete();
        }
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
                // The Azure SDK sends dead-letter reason/description in the Error.Info map.
                // Condition is "com.microsoft:dead-letter", and Info contains the user-specified
                // "DeadLetterReason" and "DeadLetterErrorDescription".
                string? dlReason = rejected.Error?.Condition?.ToString();
                string? dlDescription = rejected.Error?.Description;
                if (rejected.Error?.Info is { } info)
                {
                    // AMQPNetLite deserializes Info map keys as Symbol, not string,
                    // so we iterate and compare via ToString().
                    foreach (var key in info.Keys)
                    {
                        var keyStr = key?.ToString();
                        if (keyStr == "DeadLetterReason" && info[key] is string reason)
                            dlReason = reason;
                        if (keyStr == "DeadLetterErrorDescription" && info[key] is string desc)
                            dlDescription = desc;
                    }
                }
                _queue.DeadLetter(lockToken, dlReason, dlDescription);
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
                // AMQP Header.DeliveryCount is 0-based (number of prior unsuccessful
                // delivery attempts). The Azure SDK adds 1 to get the 1-based
                // DeliveryCount exposed on ServiceBusReceivedMessage.
                DeliveryCount = (uint)Math.Max(0, brokered.DeliveryCount - 1)
            },
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-sequence-number")] = brokered.SequenceNumber,
                [new Symbol("x-opt-enqueued-time")] = brokered.EnqueuedTimeUtc.UtcDateTime,
                [new Symbol("x-opt-lock-token")] = lockGuid,
                [new Symbol("x-opt-locked-until")] = brokered.LockedUntil != default
                    ? brokered.LockedUntil.UtcDateTime
                    : DateTimeOffset.UtcNow.Add(TimeSpan.FromMinutes(5)).UtcDateTime
            }
        };

        if (brokered.ApplicationProperties.Count > 0
            || brokered.DeadLetterReason is not null
            || brokered.DeadLetterErrorDescription is not null)
        {
            message.ApplicationProperties = new ApplicationProperties();
            foreach (var kvp in brokered.ApplicationProperties)
            {
                message.ApplicationProperties[kvp.Key] = kvp.Value;
            }

            // Azure Service Bus transmits dead-letter metadata as application properties
            if (brokered.DeadLetterReason is not null)
                message.ApplicationProperties["DeadLetterReason"] = brokered.DeadLetterReason;
            if (brokered.DeadLetterErrorDescription is not null)
                message.ApplicationProperties["DeadLetterErrorDescription"] = brokered.DeadLetterErrorDescription;
        }

        return message;
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
