using Amqp;
using Amqp.Sasl;
using Amqp.Types;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// SASL profile for the MSSBCBS (Microsoft Service Bus Claims Based Security) mechanism.
/// The Azure ServiceBusClient requires this mechanism during AMQP connection setup.
/// This implementation accepts all connections unconditionally — actual token validation
/// is handled by the CBS ($cbs) request processor after the connection is established.
/// </summary>
public class MssbcbsSaslProfile : SaslProfile
{
    public static readonly Symbol MechanismName = (Symbol)"MSSBCBS";

    public MssbcbsSaslProfile() : base(MechanismName)
    {
    }

    protected override ITransport UpgradeTransport(ITransport transport)
    {
        return transport; // No transport upgrade needed
    }

    protected override DescribedList GetStartCommand(string hostname)
    {
        // Server-side only — this is never called on the server
        return new SaslInit { Mechanism = MechanismName };
    }

    protected override DescribedList OnCommand(DescribedList command)
    {
        if (command is SaslInit)
        {
            return new SaslOutcome { Code = SaslCode.Ok };
        }

        return new SaslOutcome { Code = SaslCode.Ok };
    }
}
