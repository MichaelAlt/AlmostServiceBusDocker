using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Routes incoming AMQP link attach requests to the appropriate endpoint.
/// </summary>
public class ServiceBusLinkProcessor : ILinkProcessor
{
    private readonly NamespaceRegistry _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;

    public ServiceBusLinkProcessor(NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Process(AttachContext attachContext)
    {
        // Link.Role == true means the server-side link is a receiver (client is sending)
        // Link.Role == false means the server-side link is a sender (client is receiving)
        var isServerReceiver = attachContext.Link.Role;

        string? address;
        if (isServerReceiver)
        {
            // Client is sending: address comes from Target
            var target = attachContext.Link.Name; // fallback
            if (attachContext.Attach.Target is Target t)
                address = t.Address;
            else
                address = null;
        }
        else
        {
            // Client is receiving: address comes from Source
            if (attachContext.Attach.Source is Source s)
                address = s.Address;
            else
                address = null;
        }

        // The Azure SDK sends addresses with a leading '/' (e.g. "/my-queue").
        // Trim it to match entity names created via the REST API.
        address = address?.TrimStart('/');

        if (string.IsNullOrEmpty(address))
        {
            attachContext.Complete(new Error(new Symbol("amqp:invalid-field"))
            {
                Description = "Link address is required."
            });
            return;
        }

        // $cbs and $management are handled by ContainerHost's RegisterRequestProcessor
        if (address is "$cbs" or "$management")
        {
            attachContext.Complete(new Error(new Symbol("amqp:not-found"))
            {
                Description = $"Node '{address}' is handled as a request processor, not via link processor."
            });
            return;
        }

        var context = ResolveNamespace(attachContext);

        // Set max message size on the attach frame (256 KB, matching Azure Service Bus standard tier).
        // Without this, the SDK sees -1 and rejects all messages as too large.
        attachContext.Attach.MaxMessageSize = 256 * 1024;

        if (isServerReceiver)
        {
            // Client is sending messages to us -- auto-create entity if needed
            EnsureEntityExists(context, address);
            var endpoint = new SenderLinkEndpoint(context, address, _scheduledProcessor);
            attachContext.Complete(endpoint, 300);
        }
        else
        {
            // Client is receiving messages from us -- resolve queue
            var queue = context.ResolveQueue(address);
            if (queue is null)
            {
                attachContext.Complete(new Error(new Symbol("amqp:not-found"))
                {
                    Description = $"Queue or subscription '{address}' not found."
                });
                return;
            }

            var endpoint = new ReceiverLinkEndpoint(queue);
            attachContext.Complete(endpoint, 0);
        }
    }

    /// <summary>
    /// Resolves the namespace from the AMQP connection's hostname, matching
    /// the REST API's namespace resolution from the Host header.
    /// e.g. "app1.localhost" → namespace "app1", "localhost" → namespace "default"
    /// </summary>
    private NamespaceContext ResolveNamespace(AttachContext attachContext)
    {
        // Walk up: Link → Session → Connection to get the remote OPEN frame's HostName
        var connection = attachContext.Link.Session.Connection;

        // The Connection.Open property contains the remote peer's OPEN performative
        // which has the HostName field set by the client.
        string namespaceName = "default";

        // Try to get hostname from the connection's OPEN frame via reflection
        // AMQPNetLite doesn't expose this directly on Connection, but the
        // ListenerConnection's properties or the OPEN frame has it.
        try
        {
            // Connection stores the remote open frame — try to access it
            var openProp = connection.GetType().GetProperty("Open",
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (openProp?.GetValue(connection) is Open open && !string.IsNullOrEmpty(open.HostName))
            {
                var host = open.HostName;
                namespaceName = host.Split('.')[0];
                if (namespaceName.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                    namespaceName = "default";
            }
        }
        catch
        {
            // Fallback to default
        }

        return _registry.GetOrCreate(namespaceName);
    }

    private static void EnsureEntityExists(NamespaceContext context, string address)
    {
        // If neither a queue nor topic exists for this address, create a queue
        var (queue, topic) = context.ResolveSendTarget(address);
        if (queue is null && topic is null)
        {
            context.CreateQueue(address);
        }
    }
}
