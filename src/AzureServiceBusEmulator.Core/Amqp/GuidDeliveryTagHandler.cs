using System.Reflection;
using Amqp;
using Amqp.Handler;
using Amqp.Types;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// AMQPNetLite IHandler that rewrites outgoing delivery tags from 4-byte
/// integers to 16-byte GUIDs before they hit the wire.
///
/// The Azure SDK reads the delivery tag as LockTokenGuid — if it's not
/// 16 bytes, the SDK treats the message as "peeked" and rejects settlement.
///
/// Uses reflection because AMQPNetLite's Delivery class is internal.
/// </summary>
public class GuidDeliveryTagHandler : IHandler
{
    private static readonly PropertyInfo? TagProperty;
    private static readonly PropertyInfo? MessageProperty;

    static GuidDeliveryTagHandler()
    {
        var deliveryType = typeof(Link).Assembly.GetType("Amqp.Delivery");
        TagProperty = deliveryType?.GetProperty("Tag");
        MessageProperty = deliveryType?.GetProperty("Message");
    }

    public bool CanHandle(EventId id) => id == EventId.SendDelivery;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Context is null || TagProperty is null) return;

        try
        {
            // Read the lock token from the message's x-opt-lock-token annotation
            var message = MessageProperty?.GetValue(protocolEvent.Context) as Message;
            if (message?.MessageAnnotations?[new Symbol("x-opt-lock-token")] is Guid lockGuid)
            {
                TagProperty.SetValue(protocolEvent.Context, lockGuid.ToByteArray());
            }
            else
            {
                TagProperty.SetValue(protocolEvent.Context, Guid.NewGuid().ToByteArray());
            }
        }
        catch { /* best effort — if this fails, delivery tag stays as 4-byte int */ }
    }
}
