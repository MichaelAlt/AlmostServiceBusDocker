using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using Microsoft.Extensions.Logging;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Custom <see cref="IContainer"/> implementation that replaces AMQPNetLite's
/// <see cref="ContainerHost"/>. This fixes a crash in ContainerHost.AttachLink
/// when the client sends an Attach frame with a <see cref="global::Amqp.Transactions.Coordinator"/>
/// target (used for AMQP transactions by NServiceBus). ContainerHost blindly casts
/// attach.Target to <see cref="Target"/>, which throws an InvalidCastException.
///
/// By implementing IContainer ourselves, we can intercept Coordinator targets
/// and detach the link gracefully before the cast occurs.
/// </summary>
public class EmulatorContainer : IContainer
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<EmulatorContainer>();

    private readonly Dictionary<string, RequestProcessorEntry> _requestProcessors = new(StringComparer.OrdinalIgnoreCase);
    private ILinkProcessor? _linkProcessor;

    // Reflection accessor for AttachContext's internal constructor.
    // AttachContext(ListenerLink link, Attach attach) is internal in AMQPNetLite,
    // so we must use reflection to create instances for the ILinkProcessor.
    private static readonly ConstructorInfo? AttachContextCtor =
        typeof(AttachContext).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(ListenerLink), typeof(Attach)],
            null);

    // Reflection accessor for RequestContext's internal constructor.
    // RequestContext(ListenerLink requestLink, ListenerLink responseLink, Message request) is internal.
    private static readonly ConstructorInfo? RequestContextCtor =
        typeof(RequestContext).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(ListenerLink), typeof(ListenerLink), typeof(Message)],
            null);

    // Reflection accessor for ListenerLink.SettleOnSend which has an internal setter.
    private static readonly PropertyInfo? SettleOnSendProperty =
        typeof(ListenerLink).GetProperty("SettleOnSend");

    public X509Certificate2? ServiceCertificate => null;

    public IDictionary<string, TransportProvider> CustomTransports { get; } = new Dictionary<string, TransportProvider>();

    /// <summary>
    /// Registers an <see cref="IRequestProcessor"/> for a given address (e.g. "$cbs", "$management").
    /// </summary>
    public void RegisterRequestProcessor(string address, IRequestProcessor processor)
    {
        lock (_requestProcessors)
        {
            _requestProcessors[address] = new RequestProcessorEntry(processor);
        }
    }

    /// <summary>
    /// Registers the fallback <see cref="ILinkProcessor"/> for links that don't match
    /// any request processor address.
    /// </summary>
    public void RegisterLinkProcessor(ILinkProcessor linkProcessor)
    {
        _linkProcessor = linkProcessor;
    }

    public Message CreateMessage(ByteBuffer buffer)
    {
        return Message.Decode(buffer);
    }

    public Link CreateLink(ListenerConnection connection, ListenerSession session, Attach attach)
    {
        return new ListenerLink(session, attach);
    }

    public bool AttachLink(ListenerConnection connection, ListenerSession session, Link link, Attach attach)
    {
        var listenerLink = (ListenerLink)link;

        // Reject transaction coordinator links. This is the whole reason we replaced ContainerHost:
        // ContainerHost.AttachLink does ((Target)attach.Target).Address which throws InvalidCastException
        // when attach.Target is Coordinator.
        if (attach.Target is global::Amqp.Transactions.Coordinator)
        {
            Log.LogInformation("Rejecting transaction coordinator link '{LinkName}' — transactions not supported.", attach.LinkName);
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:not-implemented"))
            {
                Description = "AMQP transactions are not supported by the emulator."
            });
            return false;
        }

        // Resolve the address from the attach frame, matching ContainerHost's behavior:
        //   address = attach.Role ? ((Source)attach.Source).Address : ((Target)attach.Target).Address
        //
        // attach.Role == true  → remote is *receiver* → address on Source
        // attach.Role == false → remote is *sender*   → address on Target
        string? address = null;
        if (attach.Role)
        {
            if (attach.Source is Source s)
                address = s.Address;
        }
        else
        {
            if (attach.Target is Target t)
                address = t.Address;
        }

        // Check if a request processor is registered for this address.
        if (address != null)
        {
            RequestProcessorEntry? entry;
            lock (_requestProcessors)
            {
                _requestProcessors.TryGetValue(address, out entry);
            }

            if (entry != null)
            {
                AttachRequestProcessorLink(entry, listenerLink, address, attach);
                return true;
            }
        }

        // Fall back to the link processor for all other links.
        if (_linkProcessor != null)
        {
            var attachContext = CreateAttachContext(listenerLink, attach);
            if (attachContext != null)
            {
                _linkProcessor.Process(attachContext);
            }
            else
            {
                // Reflection failed — complete the attach with an error.
                Log.LogError("Failed to create AttachContext via reflection. Cannot dispatch link '{LinkName}'.", attach.LinkName);
                listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:internal-error"))
                {
                    Description = "Internal error creating link context."
                });
            }

            // Return false because the link processor completes the attach asynchronously.
            return false;
        }

        // No processor found for this address.
        if (string.IsNullOrWhiteSpace(address))
        {
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:invalid-field"))
            {
                Description = "The address field cannot be empty."
            });
        }
        else
        {
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:not-found"))
            {
                Description = $"No processor was found at {address}"
            });
        }

        return false;
    }

    /// <summary>
    /// Attaches a link to a request processor, replicating ContainerHost's internal RequestProcessor.AddLink behavior.
    /// Request processors use a request-response pattern: a receiver link (incoming requests) and a sender link (outgoing responses).
    /// </summary>
    private static void AttachRequestProcessorLink(RequestProcessorEntry entry, ListenerLink link, string address, Attach attach)
    {
        if (!link.Role)
        {
            // This is the response link (server sends responses back to client).
            // The client's attach has a Target with the reply-to address.
            var replyTo = ((Target)attach.Target).Address;

            lock (entry.ResponseLinks)
            {
                entry.ResponseLinks[replyTo] = link;
            }

            // SettleOnSend has an internal setter — use reflection.
            SettleOnSendProperty?.SetValue(link, true);
            link.InitializeSender(
                onCredit: (c, p, s) => { },
                onDispose: null,
                state: Tuple.Create(entry, replyTo));

            link.Closed += (sender, error) =>
            {
                if (sender is ListenerLink closedLink)
                {
                    var tuple = (Tuple<RequestProcessorEntry, string>)closedLink.State;
                    lock (tuple.Item1.ResponseLinks)
                    {
                        tuple.Item1.ResponseLinks.Remove(tuple.Item2);
                    }
                }
            };

            // Do NOT call CompleteAttach here — AttachLink returns true for request processors,
            // so ListenerLink.OnAttach will call CompleteAttach automatically.
        }
        else
        {
            // This is the request link (server receives requests from client).
            var processor = entry.Processor;

            link.InitializeReceiver(
                (uint)processor.Credit,
                (receiverLink, message, deliveryState, state) =>
                {
                    var rp = (RequestProcessorEntry)state;
                    DispatchRequest(receiverLink, message, rp);
                },
                entry);

            link.Closed += (sender, error) =>
            {
                if (sender is ListenerLink closedLink)
                {
                    var rp = (RequestProcessorEntry)closedLink.State;
                    lock (rp.RequestLinks)
                    {
                        rp.RequestLinks.Remove(closedLink);
                    }
                }
            };

            lock (entry.RequestLinks)
            {
                entry.RequestLinks.Add(link);
            }

            // Do NOT call CompleteAttach here — AttachLink returns true for request processors,
            // so ListenerLink.OnAttach will call CompleteAttach automatically.
        }
    }

    /// <summary>
    /// Dispatches a received request message to the IRequestProcessor, replicating
    /// ContainerHost's internal RequestProcessor.DispatchRequest behavior.
    /// </summary>
    private static void DispatchRequest(ListenerLink link, Message message, RequestProcessorEntry entry)
    {
        // Find the response link for this request.
        ListenerLink? responseLink = null;
        if (message.Properties?.ReplyTo != null)
        {
            lock (entry.ResponseLinks)
            {
                entry.ResponseLinks.TryGetValue(message.Properties.ReplyTo, out responseLink);
            }
        }

        if (responseLink == null)
        {
            // No response link — reject the message.
            link.DisposeMessage(message, new Rejected
            {
                Error = new Error(new Symbol("amqp:not-found"))
                {
                    Description = "No response link was found. Ensure the link is attached or reply-to is set on the request."
                }
            }, true);
            return;
        }

        // Accept the request.
        link.DisposeMessage(message, new Accepted(), true);

        // Create RequestContext via reflection (internal constructor).
        var context = CreateRequestContext(link, responseLink, message);
        if (context != null)
        {
            entry.Processor.Process(context);
        }
        else
        {
            Log.LogError("Failed to create RequestContext via reflection.");
        }
    }

    private static AttachContext? CreateAttachContext(ListenerLink link, Attach attach)
    {
        try
        {
            return (AttachContext?)AttachContextCtor?.Invoke([link, attach]);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Reflection error creating AttachContext.");
            return null;
        }
    }

    private static RequestContext? CreateRequestContext(ListenerLink requestLink, ListenerLink responseLink, Message message)
    {
        try
        {
            return (RequestContext?)RequestContextCtor?.Invoke([requestLink, responseLink, message]);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Reflection error creating RequestContext.");
            return null;
        }
    }

    /// <summary>
    /// Tracks the state for a registered request processor, including its
    /// request and response links (replicating ContainerHost's inner RequestProcessor class).
    /// </summary>
    internal class RequestProcessorEntry
    {
        public IRequestProcessor Processor { get; }
        public List<ListenerLink> RequestLinks { get; } = new();
        public Dictionary<string, ListenerLink> ResponseLinks { get; } = new(StringComparer.OrdinalIgnoreCase);

        public RequestProcessorEntry(IRequestProcessor processor)
        {
            Processor = processor;
        }
    }
}
