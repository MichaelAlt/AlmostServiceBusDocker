// src/AzureServiceBusEmulator.TestHost/ServiceBusEmulatorFixture.cs
using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
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
    private readonly NamespaceRegistry _registry = new();
    private readonly string _namespace;

    public int PublicPort { get; private set; }
    internal int AmqpPort { get; private set; }
    internal int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://{_namespace}.localhost:{PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture()
    {
        _namespace = $"test-{Guid.NewGuid():N}"[..20];
    }

    public async Task StartAsync()
    {
        PublicPort = GetFreePort();
        AmqpPort = GetFreePort();
        HttpPort = GetFreePort();

        // 1. Start Kestrel with HTTPS on internal port
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(HttpPort, o => o.UseHttps());
        });
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);
        await _webApp.StartAsync();

        // 2. Start AMQP on internal port
        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry);
        _amqpServer.Start();

        // 3. Start multiplexer on public port
        _multiplexerCts = new CancellationTokenSource();
        _multiplexer = new TcpMultiplexer(PublicPort, AmqpPort, HttpPort);
        _ = _multiplexer.StartAsync(_multiplexerCts.Token);
    }

    public async Task StopAsync()
    {
        if (_multiplexerCts is not null)
            await _multiplexerCts.CancelAsync();
        _amqpServer?.Stop();
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

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
