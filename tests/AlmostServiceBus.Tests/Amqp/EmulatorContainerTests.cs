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

    private static Connection NewConnectionInstance() =>
        (Connection)RuntimeHelpers.GetUninitializedObject(typeof(Connection));
}
