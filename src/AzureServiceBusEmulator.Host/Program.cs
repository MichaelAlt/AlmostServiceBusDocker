using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Hosting;
using AzureServiceBusEmulator.Core.Management;
using System.Net;
using System.Net.Sockets;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.SetMinimumLevel(LogLevel.Warning);

var publicPort = builder.Configuration.GetValue("Port", 5672);
var internalHttpsPort = GetFreePort();
var internalAmqpPort = GetFreePort();

var registry = new NamespaceRegistry();

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpsPort, o => o.UseHttps());
});

var app = builder.Build();

app.MapServiceBusManagementApi(registry);

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = internalAmqpPort }, registry);
amqpServer.Start();

var multiplexerCts = new CancellationTokenSource();
var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpsPort);

app.Lifetime.ApplicationStopping.Register(() =>
{
    multiplexerCts.Cancel();
    amqpServer.Stop();
});

_ = multiplexer.StartAsync(multiplexerCts.Token);

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  Listening: localhost:{publicPort}");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

try
{
    app.Run();
}
catch (InvalidOperationException ex) when (ex.Message.Contains("certificate"))
{
    Console.Error.WriteLine();
    Console.Error.WriteLine("Error: ASP.NET HTTPS development certificate not found.");
    Console.Error.WriteLine("Run 'dotnet dev-certs https --trust' to generate and trust the certificate.");
    Environment.Exit(1);
}

static int GetFreePort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
