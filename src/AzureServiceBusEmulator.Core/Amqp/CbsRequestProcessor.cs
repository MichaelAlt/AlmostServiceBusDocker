using System.Collections.Concurrent;
using System.Text;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Handles CBS ($cbs) token authentication requests.
/// The emulator accepts all tokens unconditionally.
/// Extracts the SharedAccessKeyName from the SAS token and stores it
/// for namespace resolution by the link processor.
/// </summary>
public class CbsRequestProcessor : IRequestProcessor
{
    // Maps connection identity → namespace name extracted from SAS key name.
    // Used by ServiceBusLinkProcessor to resolve namespaces.
    private static readonly ConcurrentDictionary<int, string> _connectionNamespaces = new();

    public int Credit => 100;

    public static string? GetNamespaceForConnection(Connection connection)
    {
        return _connectionNamespaces.TryGetValue(connection.GetHashCode(), out var ns) ? ns : null;
    }

    public static void RemoveConnection(Connection connection)
    {
        _connectionNamespaces.TryRemove(connection.GetHashCode(), out _);
    }

    public void Process(RequestContext requestContext)
    {
        TryExtractNamespace(requestContext);

        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = requestContext.Message.Properties?.MessageId
            }
        };
        requestContext.Complete(response);
    }

    private static void TryExtractNamespace(RequestContext requestContext)
    {
        try
        {
            string? token = null;
            if (requestContext.Message.Body is string s)
                token = s;
            else if (requestContext.Message.Body is byte[] bytes)
                token = Encoding.UTF8.GetString(bytes);

            if (token is null) return;

            var sknIdx = token.IndexOf("skn=", StringComparison.OrdinalIgnoreCase);
            if (sknIdx < 0) return;

            var start = sknIdx + 4;
            var end = token.IndexOf('&', start);
            var keyName = end >= 0 ? token[start..end] : token[start..];

            if (!string.IsNullOrEmpty(keyName)
                && !keyName.Equals("RootManageSharedAccessKey", StringComparison.OrdinalIgnoreCase))
            {
                var connection = requestContext.Link.Session.Connection;
                _connectionNamespaces[connection.GetHashCode()] = keyName;
            }
        }
        catch { /* ignore parsing errors */ }
    }
}
