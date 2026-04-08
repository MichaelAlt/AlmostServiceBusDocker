using Amqp;
using Amqp.Framing;
using AlmostServiceBus.Core.Amqp;
using System.Runtime.CompilerServices;

namespace AlmostServiceBus.Tests.Amqp;

public class CbsRequestProcessorTests
{
    [Fact]
    public void Credit_ReturnsPositiveValue()
    {
        var processor = new CbsRequestProcessor();
        Assert.True(processor.Credit > 0);
    }

    [Fact]
    public void NamespaceMappings_AreTrackedPerConnectionInstance()
    {
        var connection1 = NewConnectionInstance();
        var connection2 = NewConnectionInstance();

        CbsRequestProcessor.SetNamespaceForConnection(connection1, "ns-1");
        CbsRequestProcessor.SetNamespaceForConnection(connection2, "ns-2");

        Assert.Equal("ns-1", CbsRequestProcessor.GetNamespaceForConnection(connection1));
        Assert.Equal("ns-2", CbsRequestProcessor.GetNamespaceForConnection(connection2));

        CbsRequestProcessor.RemoveConnection(connection1);
        CbsRequestProcessor.RemoveConnection(connection2);
    }

    [Fact]
    public void RootManageSharedAccessKey_ClearsCustomNamespaceMapping()
    {
        var connection = NewConnectionInstance();

        CbsRequestProcessor.SetNamespaceForConnection(connection, "isolated-ns");
        Assert.Equal("isolated-ns", CbsRequestProcessor.GetNamespaceForConnection(connection));

        CbsRequestProcessor.SetNamespaceForConnection(connection, "RootManageSharedAccessKey");
        Assert.Null(CbsRequestProcessor.GetNamespaceForConnection(connection));
    }

    // Use uninitialized Connection instances purely as distinct reference-identity
    // keys for the CBS namespace table, without invoking any transport setup.
    private static Connection NewConnectionInstance() =>
        (Connection)RuntimeHelpers.GetUninitializedObject(typeof(Connection));
}
