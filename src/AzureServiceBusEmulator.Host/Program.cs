using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.SetMinimumLevel(LogLevel.Warning);

var amqpPort = builder.Configuration.GetValue("Amqp:Port", 5672);
var httpPort = builder.Configuration.GetValue("Http:Port", 5300);

var registry = new NamespaceRegistry();

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(httpPort);
});

var app = builder.Build();

app.MapServiceBusManagementApi(registry);

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = amqpPort }, registry);
amqpServer.Start();

app.Lifetime.ApplicationStopping.Register(() => amqpServer.Stop());

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  AMQP: amqp://localhost:{amqpPort}");
Console.WriteLine($"  HTTP: http://localhost:{httpPort}");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{amqpPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

app.Run();
