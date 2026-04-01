using System.Security.Cryptography.X509Certificates;
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

        // 1. Start Kestrel with plain HTTP on internal port
        //    (TLS is terminated by the multiplexer, not Kestrel)
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(HttpPort);
        });
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);
        await _webApp.StartAsync();

        // 2. Start AMQP on internal port
        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry);
        _amqpServer.Start();

        // 3. Start multiplexer on public port with TLS termination
        var cert = LoadDevCert();
        _multiplexerCts = new CancellationTokenSource();
        _multiplexer = new TcpMultiplexer(PublicPort, AmqpPort, HttpPort, cert);
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

    public static X509Certificate2 LoadDevCert()
    {
        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadOnly);
        // OID 1.3.6.1.4.1.311.84.1.1 identifies ASP.NET Core dev certs
        var certs = store.Certificates.Find(
            X509FindType.FindByExtension, "1.3.6.1.4.1.311.84.1.1", validOnly: false);
        if (certs.Count == 0)
            throw new InvalidOperationException(
                "ASP.NET HTTPS development certificate not found. " +
                "Run 'dotnet dev-certs https --trust' to generate and trust the certificate.");
        return new X509Certificate2(certs[0]);
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
