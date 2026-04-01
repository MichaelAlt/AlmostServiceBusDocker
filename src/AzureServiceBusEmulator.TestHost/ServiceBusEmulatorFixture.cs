using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace AzureServiceBusEmulator.TestHost;

public class ServiceBusEmulatorFixture : IAsyncDisposable
{
    private WebApplication? _webApp;
    private AmqpServer? _amqpServer;
    private readonly NamespaceRegistry _registry = new();
    private readonly string _namespace;

    public int AmqpPort { get; private set; }
    public int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://{_namespace}.localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture()
    {
        _namespace = $"test-{Guid.NewGuid():N}"[..20];
    }

    public async Task StartAsync()
    {
        AmqpPort = GetFreePort();
        HttpPort = GetFreePort();

        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry);
        _amqpServer.Start();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k => k.ListenLocalhost(HttpPort));
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);

        await _webApp.StartAsync();
    }

    public async Task StopAsync()
    {
        _amqpServer?.Stop();
        if (_webApp is not null)
            await _webApp.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _amqpServer?.Dispose();
        if (_webApp is not null)
            await _webApp.DisposeAsync();
    }

    public NamespaceContext GetNamespaceContext() => _registry.GetOrCreate(_namespace);

    /// <summary>
    /// Returns the "default" namespace context used by the AMQP server for link routing.
    /// </summary>
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
