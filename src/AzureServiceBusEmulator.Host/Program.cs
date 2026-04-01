using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Hosting;
using AzureServiceBusEmulator.Core.Management;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Net.Sockets;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.SetMinimumLevel(LogLevel.Warning);

var publicPort = builder.Configuration.GetValue("Port", 5672);
var amqpsPort = 5671; // Default AMQPS port — ServiceBusClient connects here
var internalHttpPort = GetFreePort();
var internalAmqpPort = GetFreePort();

var registry = new NamespaceRegistry();

// Kestrel serves plain HTTP — TLS is terminated by the multiplexer
builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpPort);
});

var app = builder.Build();

app.MapServiceBusManagementApi(registry);

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = internalAmqpPort }, registry);
amqpServer.Start();

// Load dev cert for TLS termination in the multiplexer
var cert = LoadDevCert();

var multiplexerCts = new CancellationTokenSource();

// Primary multiplexer on the public port (admin client HTTPS + plain AMQP)
var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpPort, cert);
_ = multiplexer.StartAsync(multiplexerCts.Token);

// Secondary multiplexer on the default AMQPS port (5671)
// The Azure ServiceBusClient strips the port from the connection string and
// connects to the default AMQPS port. We need to listen there too.
if (amqpsPort != publicPort)
{
    var amqpsMultiplexer = new TcpMultiplexer(amqpsPort, internalAmqpPort, internalHttpPort, cert);
    _ = amqpsMultiplexer.StartAsync(multiplexerCts.Token);
}

app.Lifetime.ApplicationStopping.Register(() =>
{
    multiplexerCts.Cancel();
    amqpServer.Stop();
});

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  Listening: localhost:{publicPort} (HTTPS/AMQP), localhost:{amqpsPort} (AMQPS)");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

app.Run();

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
