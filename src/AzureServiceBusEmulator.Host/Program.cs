using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Dashboard;
using AzureServiceBusEmulator.Core.Hosting;
using AzureServiceBusEmulator.Core.Management;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Net.Sockets;
using Vite.AspNetCore;

// ── Management API server (internal, behind TLS multiplexer) ──

var mgmtBuilder = WebApplication.CreateBuilder(args);
mgmtBuilder.Logging.SetMinimumLevel(LogLevel.Warning);

// Wire up logging for AMQP components (not DI-managed)
AmqpLog.Factory = LoggerFactory.Create(b => b
    .SetMinimumLevel(mgmtBuilder.Configuration.GetValue("Logging:LogLevel:AzureServiceBusEmulator.Amqp", LogLevel.Warning))
    .AddConsole());

var publicPort = mgmtBuilder.Configuration.GetValue("Port", 5672);
var dashboardPort = mgmtBuilder.Configuration.GetValue("DashboardPort", 15672);
var amqpsPort = 5671;
var internalHttpPort = GetFreePort();
var internalAmqpPort = GetFreePort();

var eventBus = new MessageEventBus();
var registry = new NamespaceRegistry(eventBus);

mgmtBuilder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpPort);
});

var mgmtApp = mgmtBuilder.Build();
mgmtApp.MapServiceBusManagementApi(registry);
await mgmtApp.StartAsync();

// ── Dashboard server (separate port, no route conflicts) ──

var dashBuilder = WebApplication.CreateBuilder(args);
dashBuilder.Logging.SetMinimumLevel(LogLevel.Warning);
dashBuilder.Services.AddViteServices();
dashBuilder.Services.AddCors();

dashBuilder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(dashboardPort);
});

var dashApp = dashBuilder.Build();

dashApp.UseCors(policy => policy
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader());

if (dashApp.Environment.IsDevelopment())
{
    dashApp.UseViteDevelopmentServer();
}

dashApp.UseStaticFiles();
dashApp.MapDashboardApi(registry);
dashApp.MapDashboardSse(eventBus);
dashApp.MapFallbackToFile("index.html");

await dashApp.StartAsync();

// ── AMQP server ──

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = internalAmqpPort }, registry);
amqpServer.Start();

// ── TLS multiplexers ──

var cert = LoadDevCert();
var multiplexerCts = new CancellationTokenSource();

var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpPort, cert);
_ = multiplexer.StartAsync(multiplexerCts.Token);

if (amqpsPort != publicPort)
{
    var amqpsMultiplexer = new TcpMultiplexer(amqpsPort, internalAmqpPort, internalHttpPort, cert);
    _ = amqpsMultiplexer.StartAsync(multiplexerCts.Token);
}

// Microsoft emulator compatibility: management API on port 5300
// The Azure SDK with UseDevelopmentEmulator=true expects HTTP management on 5300
var mgmtApiPort = 5300;
var mgmtMultiplexer = new TcpMultiplexer(mgmtApiPort, internalAmqpPort, internalHttpPort, cert);
_ = mgmtMultiplexer.StartAsync(multiplexerCts.Token);

// ── Shutdown ──

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  Service Bus: localhost:{publicPort} (HTTPS/AMQP), localhost:{amqpsPort} (AMQPS)");
Console.WriteLine($"  Management:  localhost:{mgmtApiPort} (HTTP)");
Console.WriteLine($"  Dashboard:   http://localhost:{dashboardPort}");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

// Block until Ctrl+C, then shut everything down quickly
var shutdownCts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdownCts.Cancel(); };

try { await Task.Delay(Timeout.Infinite, shutdownCts.Token); } catch (OperationCanceledException) { }

Console.WriteLine("Shutting down...");
multiplexerCts.Cancel();
amqpServer.Stop();

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
await Task.WhenAll(
    mgmtApp.StopAsync(timeout.Token),
    dashApp.StopAsync(timeout.Token)
);

static X509Certificate2 LoadDevCert()
{
    using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
    store.Open(OpenFlags.ReadOnly);
    var certs = store.Certificates.Find(
        X509FindType.FindByExtension, "1.3.6.1.4.1.311.84.1.1", validOnly: false);
    if (certs.Count == 0)
        throw new InvalidOperationException(
            "ASP.NET HTTPS development certificate not found. " +
            "Run 'dotnet dev-certs https --trust' to generate and trust the certificate.");
    return new X509Certificate2(certs[0]);
}

static int GetFreePort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
