using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace AzureServiceBusEmulator.Core.Hosting;

/// <summary>
/// Listens on a single public port and routes connections to either the AMQP backend
/// or the HTTP backend based on the protocol detected.
///
/// Handles three connection types:
///   1. Plain AMQP (first byte 0x41 'A') → proxy directly to AMQP backend
///   2. TLS with HTTP inside (HTTPS) → terminate TLS, proxy to plain HTTP backend
///   3. TLS with AMQP inside (AMQPS) → terminate TLS, proxy to plain AMQP backend
///
/// TLS termination uses the provided X.509 certificate. After the TLS handshake,
/// the decrypted first byte determines whether the client speaks HTTP or AMQP.
/// </summary>
public class TcpMultiplexer
{
    private const byte AmqpByte = 0x41; // 'A' — start of "AMQP\0\1\0\0"
    private const byte TlsByte = 0x16;  // TLS record type: Handshake

    private readonly int _listenPort;
    private readonly int _amqpPort;
    private readonly int _httpPort;
    private readonly X509Certificate2? _certificate;

    public TcpMultiplexer(int listenPort, int amqpPort, int httpPort, X509Certificate2? certificate = null)
    {
        _listenPort = listenPort;
        _amqpPort = amqpPort;
        _httpPort = httpPort;
        _certificate = certificate;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _listenPort);
        listener.Start();

        using var reg = ct.Register(() => listener.Stop());

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

            // Peek the first byte to determine if this is TLS or plain AMQP
            var firstByte = new byte[1];
            var read = await stream.ReadAsync(firstByte.AsMemory(0, 1), ct);
            if (read == 0)
            {
                client.Dispose();
                return;
            }

            if (firstByte[0] == AmqpByte)
            {
                // Plain AMQP — proxy directly to AMQP backend
                backend = await ConnectToBackend(_amqpPort, ct);
                var backendStream = backend.GetStream();
                await backendStream.WriteAsync(firstByte.AsMemory(0, 1), ct);
                await ProxyBidirectional(stream, backendStream, client, backend, ct);
            }
            else if (firstByte[0] == TlsByte && _certificate is not null)
            {
                // TLS connection — terminate TLS, then detect inner protocol
                await HandleTlsConnection(client, stream, firstByte, ct);
            }
            else
            {
                // Unknown protocol
                client.Dispose();
            }
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

    private async Task HandleTlsConnection(TcpClient client, NetworkStream rawStream, byte[] peekedByte, CancellationToken ct)
    {
        // Wrap the raw stream in a PrefixedStream that replays the peeked byte,
        // then wrap that in SslStream to terminate TLS.
        var prefixedStream = new PrefixedStream(rawStream, peekedByte);
        var sslStream = new SslStream(prefixedStream, leaveInnerStreamOpen: false);

        await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = _certificate,
            ClientCertificateRequired = false,
            // Advertise both HTTP and AMQP ALPN protocols
            ApplicationProtocols = [
                SslApplicationProtocol.Http2,
                SslApplicationProtocol.Http11,
                new SslApplicationProtocol("amqp"),
            ],
        }, ct);

        // After TLS handshake, determine the inner protocol.
        // First check ALPN negotiation result.
        int backendPort;
        var alpn = sslStream.NegotiatedApplicationProtocol;

        if (alpn == new SslApplicationProtocol("amqp"))
        {
            backendPort = _amqpPort;
        }
        else if (alpn == SslApplicationProtocol.Http2 || alpn == SslApplicationProtocol.Http11)
        {
            backendPort = _httpPort;
        }
        else
        {
            // No ALPN or unknown — peek the first decrypted byte to decide
            var innerByte = new byte[1];
            var read = await sslStream.ReadAsync(innerByte.AsMemory(0, 1), ct);
            if (read == 0)
            {
                sslStream.Dispose();
                return;
            }

            backendPort = innerByte[0] == AmqpByte ? _amqpPort : _httpPort;

            // Connect to backend and send the peeked inner byte
            var be = await ConnectToBackend(backendPort, ct);
            var beStream = be.GetStream();
            await beStream.WriteAsync(innerByte.AsMemory(0, 1), ct);
            await ProxyBidirectional(sslStream, beStream, client, be, ct);
            return;
        }

        // ALPN matched — proxy the decrypted stream to the backend
        var backend = await ConnectToBackend(backendPort, ct);
        var backendStream = backend.GetStream();
        await ProxyBidirectional(sslStream, backendStream, client, backend, ct);
    }

    private static async Task<TcpClient> ConnectToBackend(int port, CancellationToken ct)
    {
        var backend = new TcpClient();
        await backend.ConnectAsync(IPAddress.Loopback, port, ct);
        return backend;
    }

    private static async Task ProxyBidirectional(
        Stream clientStream, NetworkStream backendStream,
        TcpClient client, TcpClient backend, CancellationToken ct)
    {
        var clientToBackend = clientStream.CopyToAsync(backendStream, ct)
            .ContinueWith(_ =>
            {
                try { backend.Client.Shutdown(SocketShutdown.Send); } catch { }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

        var backendToClient = backendStream.CopyToAsync(clientStream, ct)
            .ContinueWith(_ =>
            {
                try { client.Client.Shutdown(SocketShutdown.Send); } catch { }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

        await Task.WhenAll(clientToBackend, backendToClient);
    }

    /// <summary>
    /// A stream wrapper that prepends previously-read bytes before the inner stream.
    /// Used to replay the peeked TLS byte so SslStream sees the complete ClientHello.
    /// </summary>
    private sealed class PrefixedStream : Stream
    {
        private readonly Stream _inner;
        private readonly byte[] _prefix;
        private int _prefixOffset;

        public PrefixedStream(Stream inner, byte[] prefix)
        {
            _inner = inner;
            _prefix = prefix;
            _prefixOffset = 0;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, count);
                Buffer.BlockCopy(_prefix, _prefixOffset, buffer, offset, toCopy);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return _inner.Read(buffer, offset, count);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, count);
                Buffer.BlockCopy(_prefix, _prefixOffset, buffer, offset, toCopy);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, offset, count, ct);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, buffer.Length);
                _prefix.AsMemory(_prefixOffset, toCopy).CopyTo(buffer);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, ct);
        }

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _inner.WriteAsync(buffer, offset, count, ct);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(buffer, ct);
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => _inner.FlushAsync(ct);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
