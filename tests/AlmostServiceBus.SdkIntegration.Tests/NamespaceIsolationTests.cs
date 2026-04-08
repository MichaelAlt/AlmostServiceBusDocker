using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class NamespaceIsolationTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task ServiceBusClient_UsesFixtureNamespace_InsteadOfDefault()
    {
        const string queueName = "isolated-queue";

        _fixture.GetNamespaceContext().CreateQueue(queueName);

        await using var client = new ServiceBusClient($"{_fixture.ConnectionString};UseDevelopmentEmulator=true");
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "isolated-msg-1" });

        var receiver = client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(received);
        Assert.Equal("isolated-msg-1", received.MessageId);
        Assert.NotNull(_fixture.GetNamespaceContext().GetQueue(queueName));
        Assert.Null(_fixture.GetDefaultNamespaceContext().GetQueue(queueName));
    }
}
