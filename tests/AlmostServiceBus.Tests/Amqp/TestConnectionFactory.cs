using System.Reflection;
using System.Runtime.CompilerServices;
using Amqp;

namespace AlmostServiceBus.Tests.Amqp;

internal static class TestConnectionFactory
{
    // Use uninitialized Connection instances purely as identity holders for AMQP
    // emulator tests, without running AMQPNetLite transport/session setup.
    internal static Connection NewConnectionInstance(ITransport? transport = null, string? containerId = null, string? remoteContainerId = null)
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
}
