using System.Reflection;
using System.Runtime.CompilerServices;
using Amqp;
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Amqp;

public class EmulatorContainerTests
{
    [Fact]
    public void TryCreateEntityManagementEntry_PrefersRequestedNamespace()
    {
        var registry = new NamespaceRegistry();
        registry.GetOrCreate("ns-1").CreateQueue("shared-queue");
        registry.GetOrCreate("ns-2").CreateQueue("shared-queue");

        var container = new EmulatorContainer();
        container.SetNamespaceRegistry(registry);

        var method = typeof(EmulatorContainer).GetMethod("TryCreateEntityManagementEntry",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        var entry = method.Invoke(container, ["shared-queue", "ns-2"]);

        Assert.NotNull(entry);

        var processor = entry!.GetType().GetProperty("Processor")!.GetValue(entry);
        var context = (NamespaceContext)processor!.GetType()
            .GetField("_context", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(processor)!;

        Assert.Equal("ns-2", context.Name);
    }

    [Fact]
    public void TryCreateEntityManagementEntry_CreatesScopedTopicEntry()
    {
        var registry = new NamespaceRegistry();
        registry.GetOrCreate("ns-1").CreateTopic("shared-topic");

        var container = new EmulatorContainer();
        container.SetNamespaceRegistry(registry);

        var method = typeof(EmulatorContainer).GetMethod("TryCreateEntityManagementEntry",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        var entry = method.Invoke(container, ["shared-topic", "ns-1"]);

        Assert.NotNull(entry);

        var processor = entry!.GetType().GetProperty("Processor")!.GetValue(entry);
        var scopedAddress = (string?)processor!.GetType()
            .GetField("_scopedAddress", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(processor);

        Assert.Equal("shared-topic", scopedAddress);
    }

    [Fact]
    public void EntityManagementProcessorKeys_AreNamespaceAware()
    {
        var connection1 = NewConnectionInstance();
        var connection2 = NewConnectionInstance();

        CbsRequestProcessor.SetNamespaceForConnection(connection1, "ns-1");
        CbsRequestProcessor.SetNamespaceForConnection(connection2, "ns-2");

        var method = typeof(EmulatorContainer).GetMethod("BuildEntityManagementProcessorKey",
            BindingFlags.Static | BindingFlags.NonPublic)!;

        var key1 = (string)method.Invoke(null, [connection1, "wolverine-dead-letter-queue/$management"])!;
        var key2 = (string)method.Invoke(null, [connection2, "wolverine-dead-letter-queue/$management"])!;

        Assert.NotEqual(key1, key2);
        Assert.Equal("ns-1|wolverine-dead-letter-queue/$management", key1);
        Assert.Equal("ns-2|wolverine-dead-letter-queue/$management", key2);

        CbsRequestProcessor.RemoveConnection(connection1);
        CbsRequestProcessor.RemoveConnection(connection2);
    }

    [Fact]
    public void SenderLinkRegistryKeys_AreScopedByConnectionIdentity()
    {
        var connection1 = NewConnectionInstance(containerId: "server-1", remoteContainerId: "client-1");
        var connection2 = NewConnectionInstance(containerId: "server-2", remoteContainerId: "client-2");

        var method = typeof(EmulatorContainer).GetMethod("BuildSenderLinkRegistryKey",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        var key1 = (string)method.Invoke(null, [connection1, "sender-link"])!;
        var key2 = (string)method.Invoke(null, [connection2, "sender-link"])!;

        Assert.NotEqual(key1, key2);
        Assert.Equal("client-1|server-1|sender-link", key1);
        Assert.Equal("client-2|server-2|sender-link", key2);
    }

    [Fact]
    public void SenderLinkRegistryKeys_AlsoIncludeNamespaceFallback()
    {
        var connection = NewConnectionInstance(containerId: "server-1", remoteContainerId: "client-1");
        CbsRequestProcessor.SetNamespaceForConnection(connection, "ns-1");

        var method = typeof(EmulatorContainer).GetMethod("BuildSenderLinkRegistryKeys",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        var keys = ((IEnumerable<string>)method.Invoke(null, [connection, "sender-link"])!).ToArray();

        Assert.Equal(["client-1|server-1|sender-link", "ns-1|sender-link"], keys);

        CbsRequestProcessor.RemoveConnection(connection);
    }

    private static Connection NewConnectionInstance(string? containerId = null, string? remoteContainerId = null)
    {
        var connection = (Connection)RuntimeHelpers.GetUninitializedObject(typeof(Connection));
        var type = typeof(Connection);

        if (containerId is not null)
            type.GetField("<ContainerId>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, containerId);

        if (remoteContainerId is not null)
            type.GetField("<RemoteContainerId>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, remoteContainerId);

        return connection;
    }
}
