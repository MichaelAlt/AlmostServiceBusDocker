using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class SubscriptionEntityTests
{
    private static BrokeredMessage CreateMessage(string? body = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes(body ?? "hello")
        };
    }

    [Fact]
    public void HasDefaultRule()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");

        var rule = sub.GetRule("$Default");

        Assert.NotNull(rule);
        Assert.Equal("$Default", rule!.Name);
        Assert.Equal(FilterType.TrueFilter, rule.FilterType);
    }

    [Fact]
    public void AddOrUpdateRule_AddsNewRule()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var rule = new RuleEntity { Name = "MyRule", FilterType = FilterType.SqlFilter, SqlExpression = "1=1" };

        sub.AddOrUpdateRule(rule);

        var retrieved = sub.GetRule("MyRule");
        Assert.NotNull(retrieved);
        Assert.Equal("MyRule", retrieved!.Name);
    }

    [Fact]
    public void RemoveRule_RemovesIt()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var rule = new RuleEntity { Name = "MyRule", FilterType = FilterType.TrueFilter };
        sub.AddOrUpdateRule(rule);

        sub.RemoveRule("MyRule");

        Assert.Null(sub.GetRule("MyRule"));
    }

    [Fact]
    public void DeliverMessage_WithoutForwardTo_EnqueuesInOwnQueue()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var message = CreateMessage("deliver-own");

        sub.DeliverMessage(message);

        var received = sub.Queue.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal(message.MessageId, received!.MessageId);
    }

    [Fact]
    public void DeliverMessage_WithForwardTo_RoutesToTargetQueue()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var targetQueue = new QueueEntity("target-queue");
        sub.ForwardTo = "target-queue";
        sub.ResolvedForwardToQueue = targetQueue;

        sub.DeliverMessage(CreateMessage("forward-me"));

        var msgInTarget = targetQueue.TryDequeueImmediate();
        var msgInOwn = sub.Queue.TryDequeueImmediate();

        Assert.NotNull(msgInTarget);
        Assert.Null(msgInOwn);
    }
}
