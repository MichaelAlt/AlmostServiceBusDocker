using global::Amqp;
using global::Amqp.Listener;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Wraps the AMQPNetLite <see cref="ContainerHost"/> lifecycle.
/// </summary>
public class AmqpServer : IDisposable
{
    private readonly AmqpServerOptions _options;
    private readonly NamespaceRegistry _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private ContainerHost? _host;

    public AmqpServer(AmqpServerOptions options, NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _options = options;
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Start()
    {
        var address = new Address(_options.Host, _options.Port, null, null, "/", "AMQP");
        _host = new ContainerHost(address);

        // Enable SASL so the Azure SDK's AMQPS connections can authenticate.
        // The SDK uses MSSBCBS (Microsoft Service Bus CBS) mechanism.
        foreach (var listener in _host.Listeners)
        {
            listener.SASL.EnableAnonymousMechanism = true;
            listener.SASL.EnablePlainMechanism("RootManageSharedAccessKey", "emulator");
            listener.SASL.EnableMechanism(MssbcbsSaslProfile.MechanismName, new MssbcbsSaslProfile());

            // Rewrite delivery tags from 4-byte ints to 16-byte GUIDs.
            // The Azure SDK reads the delivery tag as LockTokenGuid — without
            // this, PeekLock settlement (Complete/Abandon) fails.
            listener.HandlerFactory = _ => new GuidDeliveryTagHandler();
        }

        // Register CBS authentication handler
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());

        // Register management endpoint
        var defaultContext = _registry.GetOrCreate("default");
        _host.RegisterRequestProcessor("$management", new ManagementLinkEndpoint(defaultContext, _scheduledProcessor));

        // Register link processor for all other links
        _host.RegisterLinkProcessor(new ServiceBusLinkProcessor(_registry, _scheduledProcessor));

        _host.Open();
    }

    public void Stop()
    {
        _host?.Close();
        _host = null;
    }

    public void Dispose()
    {
        Stop();
    }
}
