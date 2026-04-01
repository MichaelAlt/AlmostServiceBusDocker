using System.Net;
using System.Net.Sockets;

namespace AzureServiceBusEmulator.Core.Hosting;

/// <summary>
/// Listens on a single public port and routes connections to either the AMQP backend
/// or the HTTPS backend based on the first byte of the connection.
///
/// AMQP connections start with 0x41 ('A' from the "AMQP" protocol header).
/// TLS connections start with 0x16 (TLS ClientHello record type).
/// </summary>
public class TcpMultiplexer
{
    private const byte AmqpByte = 0x41; // 'A' — start of "AMQP\0\1\0\0"
    private const byte TlsByte = 0x16;  // TLS record type: Handshake

    private readonly int _listenPort;
    private readonly int _amqpPort;
    private readonly int _httpsPort;

    public TcpMultiplexer(int listenPort, int amqpPort, int httpsPort)
    {
        _listenPort = listenPort;
        _amqpPort = amqpPort;
        _httpsPort = httpsPort;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _listenPort);
        listener.Start();

        ct.Register(() => listener.Stop());

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(ct);
                _ = HandleConnectionAsync(client, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken ct)
    {
        TcpClient? backend = null;
        try
        {
            var stream = client.GetStream();

            // Peek the first byte
            var firstByte = new byte[1];
            var read = await stream.ReadAsync(firstByte, 0, 1, ct);
            if (read == 0)
            {
                client.Dispose();
                return;
            }

            var backendPort = firstByte[0] switch
            {
                AmqpByte => _amqpPort,
                TlsByte => _httpsPort,
                _ => -1
            };

            if (backendPort == -1)
            {
                client.Dispose();
                return;
            }

            // Connect to backend
            backend = new TcpClient();
            await backend.ConnectAsync(IPAddress.Loopback, backendPort, ct);
            var backendStream = backend.GetStream();

            // Send the peeked byte first
            await backendStream.WriteAsync(firstByte, 0, 1, ct);

            // Bidirectional proxy
            var clientToBackend = stream.CopyToAsync(backendStream, ct)
                .ContinueWith(_ => backend.Client.Shutdown(SocketShutdown.Send), TaskContinuationOptions.OnlyOnRanToCompletion);
            var backendToClient = backendStream.CopyToAsync(stream, ct)
                .ContinueWith(_ => client.Client.Shutdown(SocketShutdown.Send), TaskContinuationOptions.OnlyOnRanToCompletion);

            await Task.WhenAll(clientToBackend, backendToClient);
        }
        catch
        {
            // Connection error — just clean up
        }
        finally
        {
            client.Dispose();
            backend?.Dispose();
        }
    }
}
