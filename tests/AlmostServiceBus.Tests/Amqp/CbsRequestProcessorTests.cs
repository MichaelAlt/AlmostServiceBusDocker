using Amqp;
using Amqp.Framing;
using AlmostServiceBus.Core.Amqp;
using System.Reflection;
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

    [Fact]
    public void NamespaceMappings_FallBackToTransportIdentity()
    {
        var transport = new StubTransport();
        var connection1 = NewConnectionInstance(transport);
        var connection2 = NewConnectionInstance(transport);

        CbsRequestProcessor.SetNamespaceForConnection(connection1, "ns-transport");

        Assert.Equal("ns-transport", CbsRequestProcessor.GetNamespaceForConnection(connection2));

        CbsRequestProcessor.RemoveConnection(connection1);
        Assert.Null(CbsRequestProcessor.GetNamespaceForConnection(connection2));
    }

    [Fact]
    public void NamespaceMappings_FallBackToConnectionIdentity()
    {
        var connection1 = NewConnectionInstance(containerId: "server-1", remoteContainerId: "client-1");
        var connection2 = NewConnectionInstance(containerId: "server-1", remoteContainerId: "client-1");

        CbsRequestProcessor.SetNamespaceForConnection(connection1, "ns-identity");

        Assert.Equal("ns-identity", CbsRequestProcessor.GetNamespaceForConnection(connection2));

        CbsRequestProcessor.RemoveConnection(connection1);
        Assert.Null(CbsRequestProcessor.GetNamespaceForConnection(connection2));
    }

    // Use uninitialized Connection instances purely as distinct reference-identity
    // keys for the CBS namespace table, without invoking any transport setup.
    private static Connection NewConnectionInstance(ITransport? transport = null, string? containerId = null, string? remoteContainerId = null)
    {
        var connection = (Connection)RuntimeHelpers.GetUninitializedObject(typeof(Connection));
        var type = typeof(Connection);
        if (transport is not null)
        {
            type.GetField("writer", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, transport);
        }

        if (containerId is not null)
            type.GetField("<ContainerId>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, containerId);

        if (remoteContainerId is not null)
            type.GetField("<RemoteContainerId>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, remoteContainerId);

        return connection;
    }

    private sealed class StubTransport : ITransport
    {
        public void Close() { }
        public int Receive(byte[] buffer, int offset, int count) => 0;
        public void Send(ByteBuffer buffer) { }
    }
}
