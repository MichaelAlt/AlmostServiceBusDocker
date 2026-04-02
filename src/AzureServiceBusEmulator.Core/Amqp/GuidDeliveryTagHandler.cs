using System.Reflection;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Amqp.Listener;
using Amqp.Types;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// AMQPNetLite IHandler that:
/// 1. Rewrites outgoing delivery tags from 4-byte integers to 16-byte GUIDs
///    (the Azure SDK reads the delivery tag as LockTokenGuid — if it's not
///    16 bytes, the SDK treats the message as "peeked" and rejects settlement).
/// 2. Handles connection close events to clean up CBS connection tracking.
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

    public bool CanHandle(EventId id) =>
        id == EventId.SendDelivery ||
        id == EventId.ConnectionRemoteClose ||
        id == EventId.LinkRemoteOpen;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Id == EventId.ConnectionRemoteClose)
        {
            HandleConnectionRemoteClose(protocolEvent);
            return;
        }

        if (protocolEvent.Id == EventId.LinkRemoteOpen)
        {
            // Intercept transaction coordinator links before AMQPNetLite crashes.
            // The handler fires before ContainerHost.AttachLink, giving us a chance
            // to detach the link cleanly.
            HandleLinkRemoteOpen(protocolEvent);
            return;
        }

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

    private static void HandleLinkRemoteOpen(Event protocolEvent)
    {
        try
        {
            // Check if the link's attach has a Coordinator target (transaction link).
            // If so, detach it before ContainerHost.AttachLink tries to cast it to Target.
            if (protocolEvent.Link is ListenerLink link)
            {
                var attach = link.GetType().GetField("attach",
                    System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                var attachFrame = attach?.GetValue(link) as Attach;
                if (attachFrame?.Target is global::Amqp.Transactions.Coordinator)
                {
                    link.Close(TimeSpan.Zero, new Error(new Symbol("amqp:not-implemented"))
                    {
                        Description = "AMQP transactions are not supported by the emulator."
                    });
                }
            }
        }
        catch { }
    }

    private static void HandleConnectionRemoteClose(Event protocolEvent)
    {
        try
        {
            // Clean up CBS connection tracking when a connection is closed remotely.
            if (protocolEvent.Context is Connection connection)
            {
                CbsRequestProcessor.RemoveConnection(connection);
            }
        }
        catch { /* best effort cleanup */ }
    }
}
