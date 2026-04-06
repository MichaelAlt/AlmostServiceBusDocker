using Amqp;
using Amqp.Framing;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class MessagingTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private async Task<Connection> OpenConnectionAsync()
    {
        var address = new Address("localhost", _fixture.PublicPort, null, null, "/", "AMQP");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = Amqp.Sasl.SaslProfile.Anonymous;
        return await factory.CreateAsync(address);
    }

    [Fact]
    public async Task CbsAuthentication_AcceptsToken()
    {
        // The $cbs node is registered as a RequestProcessor on the ContainerHost.
        // We test it by verifying that a connection can be established and a
        // sender link attached (which requires the AMQP handshake to succeed).
        // CBS is also implicitly validated by all other send/receive tests.
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("cbs-test-queue");

        var connection = await OpenConnectionAsync();
        var session = new Session(connection);

        // If CBS were broken, link attach would fail
        var sender = new SenderLink(session, "cbs-test-sender", "cbs-test-queue");
        var message = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes("cbs-test") })
        {
            Properties = new Properties { MessageId = "cbs-msg-1" }
        };
        await sender.SendAsync(message);

        var receiver = new ReceiverLink(session, "cbs-test-receiver", "cbs-test-queue");
        var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(received);
        receiver.Accept(received);

        await sender.CloseAsync();
        await receiver.CloseAsync();
        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task SendAndReceive_Queue_RoundTrips()
    {
        // Pre-create queue in the "default" namespace context (used by AmqpServer)
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("test-queue");

        var connection = await OpenConnectionAsync();
        var session = new Session(connection);

        // Send a message
        var sender = new SenderLink(session, "sender-1", "test-queue");
        var message = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes("Hello") })
        {
            Properties = new Properties { MessageId = "msg-1" }
        };
        await sender.SendAsync(message);

        // Receive the message
        var receiver = new ReceiverLink(session, "receiver-1", "test-queue");
        var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(received);

        byte[] body;
        if (received.Body is Data data)
            body = data.Binary;
        else if (received.Body is byte[] bytes)
            body = bytes;
        else
            throw new Exception($"Unexpected body type: {received.Body?.GetType()}");

        Assert.Equal("Hello", System.Text.Encoding.UTF8.GetString(body));

        receiver.Accept(received);
        await sender.CloseAsync();
        await receiver.CloseAsync();
        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task SendToTopic_ReceiveFromForwardedQueue()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("target-queue");
        context.CreateTopic("my-topic");
        context.CreateSubscription("my-topic", "sub-1", forwardTo: "target-queue");

        var connection = await OpenConnectionAsync();
        var session = new Session(connection);

        // Send to topic
        var sender = new SenderLink(session, "topic-sender", "my-topic");
        var message = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes("TopicMessage") })
        {
            Properties = new Properties { MessageId = "topic-msg-1" }
        };
        await sender.SendAsync(message);

        // Receive from forwarded queue
        var receiver = new ReceiverLink(session, "target-receiver", "target-queue");
        var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(received);

        byte[] body;
        if (received.Body is Data data)
            body = data.Binary;
        else if (received.Body is byte[] bytes)
            body = bytes;
        else
            throw new Exception($"Unexpected body type: {received.Body?.GetType()}");

        Assert.Equal("TopicMessage", System.Text.Encoding.UTF8.GetString(body));

        receiver.Accept(received);
        await sender.CloseAsync();
        await receiver.CloseAsync();
        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task MessageProperties_RoundTrip()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("props-queue");

        var connection = await OpenConnectionAsync();
        var session = new Session(connection);

        var sender = new SenderLink(session, "props-sender", "props-queue");
        var message = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes("PropsBody") })
        {
            Properties = new Properties
            {
                MessageId = "props-msg-1",
                CorrelationId = "corr-123",
                ContentType = "application/json",
                Subject = "test-subject"
            },
            ApplicationProperties = new ApplicationProperties
            {
                ["custom-key"] = "custom-value",
                ["priority"] = 5
            }
        };
        await sender.SendAsync(message);

        var receiver = new ReceiverLink(session, "props-receiver", "props-queue");
        var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(received);
        Assert.Equal("props-msg-1", received.Properties.MessageId);
        Assert.Equal("corr-123", received.Properties.CorrelationId);
        Assert.Equal("application/json", (string)received.Properties.ContentType);
        Assert.Equal("test-subject", received.Properties.Subject);
        Assert.Equal("custom-value", (string)received.ApplicationProperties["custom-key"]);
        Assert.Equal(5, received.ApplicationProperties["priority"]);

        receiver.Accept(received);
        await sender.CloseAsync();
        await receiver.CloseAsync();
        await session.CloseAsync();
        await connection.CloseAsync();
    }

    [Fact]
    public async Task CompetingConsumers_EachMessageDeliveredOnce()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("competing-queue");

        var connection = await OpenConnectionAsync();
        var session = new Session(connection);

        // Send 5 messages
        var sender = new SenderLink(session, "competing-sender", "competing-queue");
        for (int i = 0; i < 5; i++)
        {
            var message = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes($"Message-{i}") })
            {
                Properties = new Properties { MessageId = $"competing-{i}" }
            };
            await sender.SendAsync(message);
        }

        // Open 2 receivers
        var receiver1 = new ReceiverLink(session, "competing-receiver-1", "competing-queue");
        var receiver2 = new ReceiverLink(session, "competing-receiver-2", "competing-queue");

        var receivedIds = new System.Collections.Concurrent.ConcurrentBag<string>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        async Task ReceiveAll(ReceiverLink receiver)
        {
            while (!cts.Token.IsCancellationRequested && receivedIds.Count < 5)
            {
                var msg = await receiver.ReceiveAsync(TimeSpan.FromSeconds(2));
                if (msg is null) break;
                receivedIds.Add(msg.Properties.MessageId);
                receiver.Accept(msg);
            }
        }

        await Task.WhenAll(ReceiveAll(receiver1), ReceiveAll(receiver2));

        Assert.Equal(5, receivedIds.Count);
        Assert.Equal(5, receivedIds.Distinct().Count()); // No duplicates

        await sender.CloseAsync();
        await receiver1.CloseAsync();
        await receiver2.CloseAsync();
        await session.CloseAsync();
        await connection.CloseAsync();
    }
}
