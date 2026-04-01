using System.Net;
using System.Net.Sockets;
using System.Text;
using AzureServiceBusEmulator.Core.Hosting;
using AzureServiceBusEmulator.TestHost;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace AzureServiceBusEmulator.Tests.Hosting;

public class TcpMultiplexerTests : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Starts a TCP server that echoes back everything it receives, prefixed with a tag.
    /// </summary>
    private Task StartEchoServer(int port, string tag, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(ct);
                    _ = Task.Run(async () =>
                    {
                        using (client)
                        {
                            var stream = client.GetStream();
                            var buffer = new byte[1024];
                            var read = await stream.ReadAsync(buffer, ct);
                            var received = Encoding.UTF8.GetString(buffer, 0, read);
                            var response = Encoding.UTF8.GetBytes($"{tag}:{received}");
                            await stream.WriteAsync(response, ct);
                            client.Client.Shutdown(SocketShutdown.Send);
                        }
                    }, ct);
                }
            }
            finally
            {
                listener.Stop();
            }
        }, ct);
    }

    [Fact]
    public async Task Routes_AmqpConnection_ToAmqpBackend()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpPort, "HTTP", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with 0x41 ('A' — AMQP protocol header)
        var payload = Encoding.UTF8.GetBytes("AMQP-test-data");
        await stream.WriteAsync(payload);
        client.Client.Shutdown(SocketShutdown.Send);

        var buffer = new byte[1024];
        var read = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, read);

        Assert.StartsWith("AMQP:", response);
        Assert.Contains("AMQP-test-data", response);
    }

    [Fact]
    public async Task Closes_Connection_OnUnknownProtocol()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpPort, "HTTP", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        var payload = new byte[] { 0xFF, 0x01, 0x02 };
        await stream.WriteAsync(payload);

        var buffer = new byte[1024];
        try
        {
            var read = await stream.ReadAsync(buffer);
            Assert.Equal(0, read);
        }
        catch (IOException)
        {
            // Connection reset — also acceptable on Windows
        }
    }

    [Fact]
    public async Task Routes_Https_ThroughTlsTermination_ToPlainHttpBackend()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpPort = GetFreePort();

        // Kestrel serves plain HTTP (multiplexer handles TLS)
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k => k.ListenLocalhost(httpPort));
        builder.Logging.ClearProviders();
        var app = builder.Build();
        app.MapGet("/health", () => "ok");
        await app.StartAsync();

        try
        {
            var cert = ServiceBusEmulatorFixture.LoadDevCert();
            var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpPort, cert);
            _ = multiplexer.StartAsync(_cts.Token);

            await Task.Delay(100);

            // Client sends HTTPS — multiplexer terminates TLS, proxies HTTP to Kestrel
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            };
            using var httpClient = new HttpClient(handler)
            {
                BaseAddress = new Uri($"https://localhost:{publicPort}")
            };

            var response = await httpClient.GetStringAsync("/health");
            Assert.Equal("ok", response);
        }
        finally
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }
}
