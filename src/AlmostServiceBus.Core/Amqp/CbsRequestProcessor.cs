using System.Runtime.CompilerServices;
using System.Text;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Handles CBS ($cbs) token authentication requests.
/// The emulator accepts all tokens unconditionally.
/// Extracts the SharedAccessKeyName from the SAS token and stores it
/// for namespace resolution by the link processor.
/// </summary>
public class CbsRequestProcessor : IRequestProcessor
{
    private static readonly TimeSpan TokenExpiration = TimeSpan.FromHours(1);

    private sealed class NamespaceHolder(string value)
    {
        public string Value { get; } = value;
    }

    // Maps each live AMQP connection instance → namespace name extracted from SAS key name.
    // ConditionalWeakTable uses object identity, so separate connections can never collide
    // by hash code under heavy parallel test load.
    private static readonly ConditionalWeakTable<Connection, NamespaceHolder> _connectionNamespaces = new();

    // CBS links are long-lived and can see bursts of token renewals across many
    // parallel clients, so keep the request credit comfortably above the default.
    public int Credit => 1000;

    public static string? GetNamespaceForConnection(Connection connection)
    {
        return _connectionNamespaces.TryGetValue(connection, out var holder) ? holder.Value : null;
    }

    public static void RemoveConnection(Connection connection)
    {
        _connectionNamespaces.Remove(connection);
    }

    internal static void SetNamespaceForConnection(Connection connection, string? keyName)
    {
        _connectionNamespaces.Remove(connection);

        if (string.IsNullOrEmpty(keyName)
            || keyName.Equals("RootManageSharedAccessKey", StringComparison.OrdinalIgnoreCase))
            return;

        _connectionNamespaces.Add(connection, new NamespaceHolder(keyName));
    }

    public void Process(RequestContext requestContext)
    {
        TryExtractNamespace(requestContext);

        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK",
                // Include an expiration timestamp so the Azure SDK knows when to renew
                // the token. Without this, some SDK versions may schedule immediate
                // renewal, flooding the CBS link with requests.
                ["expiration"] = DateTime.UtcNow.Add(TokenExpiration)
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

            var connection = requestContext.Link.Session.Connection;
            SetNamespaceForConnection(connection, keyName);
        }
        catch { /* ignore parsing errors */ }
    }
}
