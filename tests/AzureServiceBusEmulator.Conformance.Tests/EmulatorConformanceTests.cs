using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AzureServiceBusEmulator.TestHost;

namespace AzureServiceBusEmulator.Conformance.Tests;

/// <summary>
/// Runs all conformance tests against the in-process emulator.
/// </summary>
public class EmulatorConformanceTests : ConformanceTestBase
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    protected override async Task<(ServiceBusClient? client, ServiceBusAdministrationClient? admin)> CreateClientsAsync()
    {
        await _fixture.StartAsync();

        // --- ServiceBusClient (AMQP over TLS) ---
        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 0,
                TryTimeout = TimeSpan.FromSeconds(10)
            }
        };

        var connectionString =
            $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

        var client = new ServiceBusClient(connectionString, clientOptions);

        // --- ServiceBusAdministrationClient (HTTPS with cert bypass + DNS redirect) ---
        var handler = new SocketsHttpHandler
        {
            SslOptions = { RemoteCertificateValidationCallback = (_, _, _, _) => true },
            ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };

        var adminOptions = new ServiceBusAdministrationClientOptions();
        adminOptions.Transport = new HttpClientTransport(new HttpClient(handler));

        var admin = new ServiceBusAdministrationClient(connectionString, adminOptions);

        return (client, admin);
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _fixture.DisposeAsync();
    }
}
