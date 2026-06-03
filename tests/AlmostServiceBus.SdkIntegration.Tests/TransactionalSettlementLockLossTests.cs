using System.Transactions;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// What happens when a message's PeekLock is lost (slow processing / thread-pool starvation
/// under load) while it is being settled — non-transactionally vs inside a transaction.
///
/// A transactional Complete is buffered and applied at commit. The commit must not silently
/// drop it: if the lock is gone by commit time the discharge is rejected so the client sees
/// the failure, rather than the broker reporting a clean commit that quietly left the message
/// un-consumed (which otherwise surfaces downstream as an "untouched" message reappearing
/// with a bumped DeliveryCount even though the operation reported success).
/// </summary>
public class TransactionalSettlementLockLossTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient() =>
        new($"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true",
            new ServiceBusClientOptions
            {
                TransportType         = ServiceBusTransportType.AmqpTcp,
                CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
                RetryOptions          = new ServiceBusRetryOptions(),
            });

    /// <summary>
    /// Baseline (correct behaviour): a NON-transactional Complete on a message whose lock
    /// has been lost surfaces the failure to the caller as a ServiceBusException, and the
    /// message is NOT consumed — so a caller can react (retry / report).
    /// </summary>
    [Fact]
    public async Task NonTransactional_complete_on_lost_lock_throws_and_leaves_message_unconsumed()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        var queue = ctx.CreateQueue("locklost-plain");
        queue.LockDuration = TimeSpan.FromSeconds(1);

        await using var client = CreateClient();
        await client.CreateSender("locklost-plain").SendMessageAsync(new ServiceBusMessage("m") { MessageId = "m1" });

        var receiver = client.CreateReceiver("locklost-plain", new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        await Task.Delay(TimeSpan.FromSeconds(2)); // lose the lock before settling

        await Assert.ThrowsAnyAsync<ServiceBusException>(() => receiver.CompleteMessageAsync(msg!));
        Assert.Equal(0, queue.ConsumedCount);
    }

    /// <summary>
    /// Regression for the fix: a Complete buffered inside a transaction whose lock is lost
    /// before commit must NOT be silently dropped. The commit's prepare phase sees the lock is
    /// gone, rolls the transaction back, and the coordinator rejects the discharge — so the SDK
    /// surfaces the failure (committing the scope throws) instead of reporting a clean commit
    /// that quietly left the message un-consumed.
    /// </summary>
    [Fact]
    public async Task Transactional_complete_on_lost_lock_is_surfaced_not_silently_committed()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        var queue = ctx.CreateQueue("locklost-txn");
        queue.LockDuration = TimeSpan.FromSeconds(1);

        await using var client = CreateClient();
        await client.CreateSender("locklost-txn").SendMessageAsync(new ServiceBusMessage("orig") { MessageId = "orig" });

        var receiver = client.CreateReceiver("locklost-txn", new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        await Task.Delay(TimeSpan.FromSeconds(2)); // lose the lock before commit

        // Committing the scope now surfaces the rejected discharge instead of succeeding silently.
        var error = await Record.ExceptionAsync(async () =>
        {
            using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
            await receiver.CompleteMessageAsync(msg!);
            scope.Complete();
        });

        Assert.NotNull(error);
        // The message was not consumed — but the caller is told, rather than it being dropped silently.
        Assert.Equal(0, queue.ConsumedCount);
    }
}
