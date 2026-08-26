using System.Reflection;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;
using BrokerSessionState = AlmostServiceBus.Core.Broker.SessionState;
using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Routes incoming AMQP link attach requests to the appropriate endpoint.
/// </summary>
public class ServiceBusLinkProcessor : ILinkProcessor
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<ServiceBusLinkProcessor>();

    // Cache PropertyInfo to avoid reflecting on every attach frame
    private static readonly PropertyInfo? OpenProperty =
        typeof(Connection).GetProperty("Open", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

    private readonly NamespaceRegistry _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly Broker.Transactions.TransactionManager? _transactions;

    public ServiceBusLinkProcessor(
        NamespaceRegistry registry,
        ScheduledMessageProcessor? scheduledProcessor = null,
        Broker.Transactions.TransactionManager? transactions = null)
    {
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
        _transactions = transactions;
    }

    public void Process(AttachContext attachContext)
    {
        Log.LogDebug(
                "RECEIVED AMQP ATTACH LINK: LinkName={LinkName}, Role={Role}, Target={Target}, Source={Source}",
                attachContext.Link.Name,
                attachContext.Link.Role ? "Receiver(ClientSender)" : "Sender(ClientReceiver)",
                (attachContext.Attach.Target as Target)?.Address,
                (attachContext.Attach.Source as Source)?.Address
            );

        // Link.Role == true means the server-side link is a receiver (client is sending)
        // Link.Role == false means the server-side link is a sender (client is receiving)
        var isServerReceiver = attachContext.Link.Role;

        string? address;
        if (isServerReceiver)
        {
            // Client is sending: address comes from Target
            address = attachContext.Attach.Target is Target t ? t.Address : attachContext.Link.Name;
        }
        else
        {
            // Client is receiving: address comes from Source
            address = attachContext.Attach.Source is Source s ? s.Address : null;
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

        // $cbs and $management are handled by EmulatorContainer's request processors
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
            var endpoint = new SenderLinkEndpoint(context, address, _scheduledProcessor, _transactions);
            attachContext.Complete(endpoint, 300);
        }
        else
        {
            // Cross-entity transactions pin the connection to its first receiver's entity. Real
            // Azure Service Bus rejects a later receiver on a different top-level entity with
            // "Local transactions cannot span multiple top-level entities" — even outside an active
            // transaction (this is what breaks a shared cross-entity client reused to peek/receive
            // across queues). Senders are unaffected: they are transferred "via" the pinned entity.
            if (!CrossEntityTransactionTracker.TryAdmitReceiver(
                    attachContext.Link.Session.Connection, address, out var pinnedEntity))
            {
                attachContext.Complete(new Error(new Symbol("com.microsoft:operation-cancelled"))
                {
                    Description =
                        "Local transactions cannot span multiple top-level entities such as queue or topic. " +
                        $"The connection is pinned to '{pinnedEntity}' because cross-entity transactions are enabled; " +
                        $"a receiver on '{address}' is not allowed. Use a separate client per entity for non-transactional reads."
                });
                return;
            }

            // Check for session filter on receiver link.
            var sessionFilterKey = new Symbol("com.microsoft:session-filter");
            string? requestedSessionId = null;
            bool hasSessionFilter = false;

            if (attachContext.Attach.Source is Source src && src.FilterSet is Map filterMap)
            {
                if (filterMap.ContainsKey(sessionFilterKey))
                {
                    var raw = filterMap[sessionFilterKey];
                    hasSessionFilter = true;
                    requestedSessionId = raw switch
                    {
                        string s when string.IsNullOrEmpty(s) => null,
                        string s => s,
                        DescribedValue dv when string.IsNullOrEmpty(dv.Value as string) => null,
                        DescribedValue dv => dv.Value as string,
                        _ => null,
                    };
                }
                else
                {
                    foreach (var kvp in filterMap)
                    {
                        if (kvp.Value is DescribedValue dv && dv.Descriptor is Symbol sym && (string)sym == "com.microsoft:session-filter")
                        {
                            hasSessionFilter = true;
                            requestedSessionId = string.IsNullOrEmpty(dv.Value as string) ? null : dv.Value as string;
                            break;
                        }
                    }
                }
            }

            if (hasSessionFilter)
            {
                HandleSessionReceiver(attachContext, context, address, requestedSessionId);
                return;
            }

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

            if (queue.RequiresSession)
            {
                attachContext.Complete(new Error(new Symbol("com.microsoft:session-required"))
                {
                    Description = $"The entity '{address}' requires session-aware receivers."
                });
                return;
            }

            var preSettled = (byte)attachContext.Attach.SndSettleMode == 1;
            var endpoint = new ReceiverLinkEndpoint(queue, preSettled, _transactions);
            attachContext.Complete(endpoint, 0);
        }
    }

    private void HandleSessionReceiver(AttachContext attachContext, NamespaceContext ns, string address, string? requestedSessionId)
    {
        var queue = ns.ResolveQueue(address);
        if (queue is null || !queue.RequiresSession || queue.Sessions is null)
        {
            attachContext.Complete(new Error(new Symbol("amqp:not-found"))
            {
                Description = $"Session-enabled queue '{address}' not found."
            });
            return;
        }

        attachContext.Attach.MaxMessageSize = 256 * 1024;
        var receiverId = attachContext.Link.Name ?? Guid.NewGuid().ToString();
        Log.LogDebug("HandleSessionReceiver: requested={Requested}, queue={Queue}, receiverId={ReceiverId}",
            requestedSessionId, address, receiverId);
        
        var session = queue.Sessions.TryAcceptSession(requestedSessionId, receiverId);

        if (session is not null)
        {
            Log.LogDebug("HandleSessionReceiver: ACCEPTED session={SessionId} immediately for receiver={ReceiverId}",
                session.SessionId, receiverId);
            CompleteSessionAttach(attachContext, queue, session);
            return;
        }

        if (!string.IsNullOrEmpty(requestedSessionId) && queue.Sessions.IsSessionLocked(requestedSessionId))
        {
            attachContext.Complete(new Error(new Symbol("com.microsoft:session-cannot-be-locked"))
            {
                Description = $"Session '{requestedSessionId}' is locked by another receiver."
            });
            return;
        }

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(65));
        
        // Match AMQPNetLite's ClosedCallback signature: void (IAmqpObject sender, Error error)
        ClosedCallback onLinkClosed = (_, _) => cts.Cancel();
        attachContext.Link.Closed += onLinkClosed;

        Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    await Task.Delay(500, cts.Token);

                    var accepted = queue.Sessions.TryAcceptSession(requestedSessionId, receiverId);
                    if (accepted is not null)
                    {
                        if (cts.IsCancellationRequested)
                        {
                            queue.Sessions.ReleaseSession(accepted.SessionId);
                            Log.LogDebug("HandleSessionReceiver: client disconnected after accepting session={SessionId}, released lock",
                                accepted.SessionId);
                            return;
                        }

                        Log.LogDebug("HandleSessionReceiver: POLL ACCEPTED session={SessionId} for receiver={ReceiverId}",
                            accepted.SessionId, receiverId);

                        try
                        {
                            CompleteSessionAttach(attachContext, queue, accepted);
                        }
                        catch (Exception ex)
                        {
                            queue.Sessions.ReleaseSession(accepted.SessionId);
                            Log.LogWarning(ex,
                                "HandleSessionReceiver: CompleteSessionAttach failed for session={SessionId}, released lock",
                                accepted.SessionId);
                        }
                        return;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Log.LogWarning(ex, "HandleSessionReceiver: polling loop failed for queue={Queue}, receiverId={ReceiverId}",
                    address, receiverId);
            }
            finally
            {
                attachContext.Link.Closed -= onLinkClosed;
                cts.Dispose();
            }

            if (!attachContext.Link.IsClosed)
            {
                try
                {
                    attachContext.Complete(new Error(new Symbol("com.microsoft:timeout"))
                    {
                        Description = requestedSessionId is not null
                            ? $"Session '{requestedSessionId}' is not available."
                            : "No sessions are available."
                    });
                }
                catch (Exception ex)
                {
                    Log.LogDebug(ex, "HandleSessionReceiver: failed to send timeout error (link likely already closed)");
                }
            }
        });
    }

    private void CompleteSessionAttach(AttachContext attachContext, QueueEntity queue, BrokerSessionState session)
    {
        try
        {
            queue.ReclaimPendingForSession(session.SessionId);
        }
        catch (Exception ex)
        {
            Log.LogDebug(ex, "ReclaimPendingForSession failed for session '{SessionId}'", session.SessionId);
        }

        attachContext.Attach.Properties = new Fields
        {
            [new Symbol("com.microsoft:locked-until-utc")] = session.LockedUntil.UtcTicks,
            [new Symbol("com.microsoft:session-id")] = session.SessionId
        };

        if (attachContext.Attach.Source is Source src)
        {
            src.FilterSet ??= new Map();
            src.FilterSet[new Symbol("com.microsoft:session-filter")] = session.SessionId;
        }

        var preSettled = (byte)attachContext.Attach.SndSettleMode == 1;
        var endpoint = new SessionReceiverLinkEndpoint(queue, session, preSettled, _transactions);
        attachContext.Complete(endpoint, 0);
    }

    private NamespaceContext ResolveNamespace(AttachContext attachContext)
    {
        var connection = attachContext.Link.Session.Connection;

        var keyName = CbsRequestProcessor.GetNamespaceForConnection(connection);
        if (keyName is not null)
        {
            return _registry.GetOrCreate(keyName);
        }

        try
        {
   
            if (OpenProperty?.GetValue(connection) is Open open && !string.IsNullOrEmpty(open.HostName))
            {
                Console.WriteLine(open.HostName);
                var host = open.HostName;
                if (!EmulatorNetwork.IsDefaultNamespaceHost(host))
                {
                    var namespaceName = host.Split('.')[0];
                    return _registry.GetOrCreate(namespaceName);
                }
            }
        }
        catch { }

        return _registry.GetOrCreate("default");
    }

    private static void EnsureEntityExists(NamespaceContext context, string address)
    {
        var (queue, topic) = context.ResolveSendTarget(address);
        if (queue is null && topic is null)
        {
            context.CreateQueue(address);
        }
    }
}