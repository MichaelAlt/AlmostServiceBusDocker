using System.Net;
using System.Net.Sockets;
using System.Text;
using AzureServiceBusEmulator.Core.Hosting;

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
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100); // let servers bind

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
    public async Task Routes_TlsConnection_ToHttpsBackend()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with 0x16 (TLS ClientHello)
        var payload = new byte[] { 0x16, 0x03, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05 };
        await stream.WriteAsync(payload);
        client.Client.Shutdown(SocketShutdown.Send);

        var buffer = new byte[1024];
        var read = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, read);

        Assert.StartsWith("HTTPS:", response);
    }

    [Fact]
    public async Task Closes_Connection_OnUnknownProtocol()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with an unknown byte
        var payload = new byte[] { 0xFF, 0x01, 0x02 };
        await stream.WriteAsync(payload);

        // Connection should be closed by multiplexer.
        // Depending on timing, this manifests as either 0 bytes read (graceful close)
        // or an IOException/SocketException (connection reset).
        var buffer = new byte[1024];
        try
        {
            var read = await stream.ReadAsync(buffer);
            Assert.Equal(0, read); // connection closed gracefully
        }
        catch (IOException)
        {
            // Connection was reset by the remote host — also acceptable
        }
    }
}
