using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Dashboard;
using AzureServiceBusEmulator.Core.Hosting;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace AzureServiceBusEmulator.TestHost;

public class ServiceBusEmulatorFixture : IAsyncDisposable
{
    private WebApplication? _webApp;
    private AmqpServer? _amqpServer;
    private TcpMultiplexer? _multiplexer;
    private CancellationTokenSource? _multiplexerCts;
    private ScheduledMessageProcessor? _scheduledProcessor;
    private readonly MessageEventBus _eventBus = new();
    private readonly NamespaceRegistry _registry;
    private readonly string _namespace;

    public int PublicPort { get; private set; }
    internal int AmqpPort { get; private set; }
    internal int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://localhost:{PublicPort};SharedAccessKeyName={_namespace};SharedAccessKey=emulator";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture()
    {
        _namespace = $"test-{Guid.NewGuid():N}"[..20];
        _registry = new NamespaceRegistry(_eventBus);
    }

    public async Task StartAsync()
    {
        EmulatorInfrastructure.EnsureDevCertTrusted();

        PublicPort = EmulatorInfrastructure.GetFreePort();
        AmqpPort = EmulatorInfrastructure.GetFreePort();
        HttpPort = EmulatorInfrastructure.GetFreePort();

        // 1. Start Kestrel with plain HTTP on internal port
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(HttpPort);
        });
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);
        _webApp.MapDashboardApi(_registry);
        _webApp.MapDashboardSse(_eventBus);
        await _webApp.StartAsync();

        // 2. Start scheduled message processor
        _scheduledProcessor = new ScheduledMessageProcessor(_registry.GetOrCreate("default"));
        _scheduledProcessor.StartBackground(TimeSpan.FromMilliseconds(500));

        // 3. Start AMQP on internal port (with SASL for Azure SDK compatibility)
        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry, _scheduledProcessor);
        _amqpServer.Start();

        // 4. Start multiplexer on public port with TLS termination
        var cert = EmulatorInfrastructure.LoadDevCert();
        _multiplexerCts = new CancellationTokenSource();
        _multiplexer = new TcpMultiplexer(PublicPort, AmqpPort, HttpPort, cert);
        _ = _multiplexer.StartAsync(_multiplexerCts.Token);
    }

    public async Task StopAsync()
    {
        if (_multiplexerCts is not null)
            await _multiplexerCts.CancelAsync();
        _amqpServer?.Stop();
        _scheduledProcessor?.Dispose();
        if (_webApp is not null)
            await _webApp.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _amqpServer?.Dispose();
        _multiplexerCts?.Dispose();
        if (_webApp is not null)
            await _webApp.DisposeAsync();
    }

    public NamespaceContext GetNamespaceContext() => _registry.GetOrCreate(_namespace);

    public NamespaceContext GetDefaultNamespaceContext() => _registry.GetOrCreate("default");
}
