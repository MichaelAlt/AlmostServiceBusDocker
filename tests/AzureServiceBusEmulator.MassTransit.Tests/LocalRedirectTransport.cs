using Azure.Core;
using Azure.Core.Pipeline;

namespace AzureServiceBusEmulator.MassTransit.Tests;

/// <summary>
/// Custom HTTP pipeline transport that redirects all Azure SDK admin client requests
/// to the local emulator. This is the same approach a real integration would use
/// to make <see cref="Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient"/>
/// talk to the emulator instead of Azure.
///
/// The transport rewrites the request URI to point to localhost:{port} while preserving
/// the original Host header (which the emulator uses for namespace resolution).
/// </summary>
public class LocalRedirectTransport : HttpPipelineTransport
{
    private readonly int _port;
    private readonly HttpClientTransport _inner;

    public LocalRedirectTransport(int port)
    {
        _port = port;
        _inner = new HttpClientTransport(new HttpClient());
    }

    public override Request CreateRequest()
    {
        return _inner.CreateRequest();
    }

    public override void Process(HttpMessage message)
    {
        RewriteUri(message);
        _inner.Process(message);
    }

    public override ValueTask ProcessAsync(HttpMessage message)
    {
        RewriteUri(message);
        return _inner.ProcessAsync(message);
    }

    private void RewriteUri(HttpMessage message)
    {
        var request = message.Request;
        var originalUri = request.Uri.ToUri();

        // Preserve original host as the Host header for namespace resolution.
        // The emulator's ResolveNamespace reads Host header to find the namespace.
        request.Headers.SetValue("Host", originalUri.Host);

        // Rewrite to localhost
        var builder = new UriBuilder(originalUri)
        {
            Scheme = "http",
            Host = "localhost",
            Port = _port
        };
        request.Uri.Reset(builder.Uri);
    }
}
