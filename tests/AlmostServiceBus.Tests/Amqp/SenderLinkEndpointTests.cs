using Amqp;
using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Amqp;

public class SenderLinkEndpointTests
{
    private static NamespaceContext CreateNamespace()
    {
        return new NamespaceContext("test");
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsBody()
    {
        var body = System.Text.Encoding.UTF8.GetBytes("hello world");
        var amqpMessage = new Message(new Data { Binary = body });

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal(body, brokered.Body);
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsProperties()
    {
        var amqpMessage = new Message("test")
        {
            Properties = new Properties
            {
                MessageId = "msg-123",
                CorrelationId = "corr-456",
                ContentType = "application/json",
                Subject = "test-subject",
                ReplyTo = "reply-queue",
                To = "dest-queue",
                GroupId = "session-1",
                ReplyToGroupId = "reply-session-1"
            }
        };

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal("msg-123", brokered.MessageId);
        Assert.Equal("corr-456", brokered.CorrelationId);
        Assert.Equal("application/json", brokered.ContentType);
        Assert.Equal("test-subject", brokered.Subject);
        Assert.Equal("reply-queue", brokered.ReplyTo);
        Assert.Equal("dest-queue", brokered.To);
        Assert.Equal("session-1", brokered.SessionId);
        Assert.Equal("reply-session-1", brokered.ReplyToSessionId);
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsApplicationProperties()
    {
        var amqpMessage = new Message("test")
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["key1"] = "value1",
                ["key2"] = 42
            }
        };

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal("value1", brokered.ApplicationProperties["key1"]);
        Assert.Equal(42, brokered.ApplicationProperties["key2"]);
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsScheduledEnqueueTime()
    {
        var scheduledTime = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
        var amqpMessage = new Message("test")
        {
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-scheduled-enqueue-time")] = scheduledTime
            }
        };

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal(scheduledTime, brokered.ScheduledEnqueueTimeUtc);
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsPartitionKey()
    {
        var amqpMessage = new Message("test")
        {
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-partition-key")] = "pk-123"
            }
        };

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal("pk-123", brokered.PartitionKey);
    }

    [Fact]
    public void ConvertToBrokeredMessage_ExtractsTtl()
    {
        var amqpMessage = new Message("test")
        {
            Header = new Header { Ttl = 60000 }
        };

        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(amqpMessage);

        Assert.Equal(TimeSpan.FromMilliseconds(60000), brokered.TimeToLive);
    }

    [Fact]
    public void RouteMessage_ToQueue_EnqueuesMessage()
    {
        var context = CreateNamespace();
        var queue = context.CreateQueue("test-queue");
        var endpoint = new SenderLinkEndpoint(context, "test-queue");

        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello")
        };

        endpoint.RouteMessage("test-queue", message);

        var dequeued = queue.TryDequeueImmediate();
        Assert.NotNull(dequeued);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(dequeued.Body));
    }

    [Fact]
    public void RouteMessage_ToTopic_FansOut()
    {
        var context = CreateNamespace();
        var topic = context.CreateTopic("test-topic");
        var sub = context.CreateSubscription("test-topic", "sub1");
        var endpoint = new SenderLinkEndpoint(context, "test-topic");

        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello")
        };

        endpoint.RouteMessage("test-topic", message);

        var dequeued = sub.Queue.TryDequeueImmediate();
        Assert.NotNull(dequeued);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(dequeued.Body));
    }

    [Fact]
    public void RouteMessage_AssignsSequenceNumber()
    {
        var context = CreateNamespace();
        context.CreateQueue("test-queue");
        var endpoint = new SenderLinkEndpoint(context, "test-queue");

        var message1 = new BrokeredMessage();
        var message2 = new BrokeredMessage();

        endpoint.RouteMessage("test-queue", message1);
        endpoint.RouteMessage("test-queue", message2);

        Assert.True(message1.SequenceNumber > 0);
        Assert.True(message2.SequenceNumber > message1.SequenceNumber);
    }

    [Fact]
    public void RouteMessage_ThrowsForUnknownAddress()
    {
        var context = CreateNamespace();
        var endpoint = new SenderLinkEndpoint(context, "nonexistent");

        var message = new BrokeredMessage();

        Assert.Throws<InvalidOperationException>(() =>
            endpoint.RouteMessage("nonexistent", message));
    }

    [Fact]
    public void RouteMessage_ScheduledMessage_UsesScheduledProcessor()
    {
        var context = CreateNamespace();
        context.CreateQueue("test-queue");
        var processor = new ScheduledMessageProcessor(context);
        var endpoint = new SenderLinkEndpoint(context, "test-queue", processor);

        var message = new BrokeredMessage
        {
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddHours(1)
        };

        endpoint.RouteMessage("test-queue", message);

        // Should not be in the queue yet (it's scheduled)
        var dequeued = context.GetQueue("test-queue")!.TryDequeueImmediate();
        Assert.Null(dequeued);

        // The message should have a sequence number assigned by the scheduler
        Assert.True(message.SequenceNumber > 0);
    }
}
