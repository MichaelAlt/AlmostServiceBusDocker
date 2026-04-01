using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Handles CBS ($cbs) token authentication requests.
/// The emulator accepts all tokens unconditionally.
/// </summary>
public class CbsRequestProcessor : IRequestProcessor
{
    public int Credit => 100;

    public void Process(RequestContext requestContext)
    {
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
}
