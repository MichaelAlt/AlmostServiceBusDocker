using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class ScheduledMessageProcessorTests
{
    private static NamespaceContext CreateNamespace() => new("test-ns");

    private static BrokeredMessage CreateMessage(DateTimeOffset? scheduledTime = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            ScheduledEnqueueTimeUtc = scheduledTime
        };
    }

    [Fact]
    public void Schedule_ReturnsSequenceNumber_GreaterThanZero()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var seqNo = processor.Schedule("my-queue", CreateMessage());

        Assert.True(seqNo > 0);
    }

    [Fact]
    public void CancelScheduled_ReturnsTrueIfFound()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var seqNo = processor.Schedule("my-queue", CreateMessage());
        var result = processor.CancelScheduled(seqNo);

        Assert.True(result);
    }

    [Fact]
    public void CancelScheduled_ReturnsFalseIfNotFound()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var result = processor.CancelScheduled(99999L);

        Assert.False(result);
    }

    [Fact]
    public void ProcessDueMessages_DeliversWhenDue()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        // Schedule a message with a time in the past
        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.NotNull(delivered);
    }

    [Fact]
    public void ProcessDueMessages_DoesNotDeliverFutureMessages()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        // Schedule a message 1 hour in the future
        var futureTime = DateTimeOffset.UtcNow.AddHours(1);
        processor.Schedule("my-queue", CreateMessage(futureTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.Null(delivered);
    }

    [Fact]
    public void ProcessDueMessages_ClearsScheduledEnqueueTimeUtc_WhenDelivered()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.NotNull(delivered);
        Assert.Null(delivered!.ScheduledEnqueueTimeUtc);
    }

    [Fact]
    public void ScheduleToTopic_FansOutWhenDue()
    {
        var ns = CreateNamespace();
        var targetQueue = ns.CreateQueue("target-queue");

        // Create topic with a subscription that forwards to target-queue
        ns.CreateSubscription("my-topic", "sub1", forwardTo: "target-queue");

        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-topic", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = targetQueue.TryDequeueImmediate();
        Assert.NotNull(delivered);
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);
        processor.StartBackground(TimeSpan.FromSeconds(1));

        // Dispose should not throw
        processor.Dispose();
    }

    [Fact]
    public void CancelScheduled_AfterProcess_ReturnsFalse()
    {
        var ns = CreateNamespace();
        ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        var seqNo = processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        // Message was already delivered, so cancellation should return false
        var result = processor.CancelScheduled(seqNo);
        Assert.False(result);
    }
}
