using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Xunit.Sdk;

namespace AzureServiceBusEmulator.Conformance.Tests;

/// <summary>
/// Base class for conformance tests that run against both the emulator and real Azure Service Bus.
/// Each test creates its own entities (GUID-based names) and cleans them up after.
/// </summary>
public abstract class ConformanceTestBase : IAsyncLifetime
{
    protected ServiceBusClient Client { get; private set; } = null!;
    protected ServiceBusAdministrationClient AdminClient { get; private set; } = null!;

    private readonly string _uniqueId = Guid.NewGuid().ToString("N")[..12];
    private readonly List<string> _createdQueues = [];
    private readonly List<string> _createdTopics = [];

    /// <summary>
    /// When non-null, all tests in this class should be skipped with this reason.
    /// </summary>
    protected string? SkipReason { get; set; }

    /// <summary>
    /// Subclasses provide the connection setup. Return null clients and set SkipReason
    /// to skip all tests.
    /// </summary>
    protected abstract Task<(ServiceBusClient? client, ServiceBusAdministrationClient? admin)> CreateClientsAsync();

    /// <summary>
    /// Throws <see cref="SkipException"/> if SkipReason is set.
    /// Call at the start of each test method.
    /// Note: xunit.runner.visualstudio 3.x may report dynamic skips as failures
    /// in the VSTest output, but the $XunitDynamicSkip$ message prefix is standard.
    /// </summary>
    protected void ThrowIfSkipped()
    {
        if (SkipReason is not null)
            throw SkipException.ForSkip(SkipReason);
    }

    public async Task InitializeAsync()
    {
        var (client, admin) = await CreateClientsAsync();
        if (client is not null)
            Client = client;
        if (admin is not null)
            AdminClient = admin;
    }

    public async Task DisposeAsync()
    {
        if (AdminClient is not null)
        {
            // Clean up entities in reverse order (subscriptions are deleted with topics)
            foreach (var topic in _createdTopics)
            {
                try { await AdminClient.DeleteTopicAsync(topic); } catch { /* best effort */ }
            }

            foreach (var queue in _createdQueues)
            {
                try { await AdminClient.DeleteQueueAsync(queue); } catch { /* best effort */ }
            }
        }

        if (Client is not null)
            await Client.DisposeAsync();
    }

    /// <summary>
    /// Creates a unique queue name and registers it for cleanup.
    /// </summary>
    protected async Task<string> CreateTestQueueAsync(CreateQueueOptions? options = null)
    {
        var name = $"ct-{_uniqueId}-q{_createdQueues.Count}";
        if (options is not null)
        {
            options.Name = name;
            await AdminClient.CreateQueueAsync(options);
        }
        else
        {
            await AdminClient.CreateQueueAsync(name);
        }

        _createdQueues.Add(name);
        return name;
    }

    /// <summary>
    /// Creates a unique topic name and registers it for cleanup.
    /// </summary>
    protected async Task<string> CreateTestTopicAsync()
    {
        var name = $"ct-{_uniqueId}-t{_createdTopics.Count}";
        await AdminClient.CreateTopicAsync(name);
        _createdTopics.Add(name);
        return name;
    }

    /// <summary>
    /// Creates a subscription on a topic, optionally with ForwardTo.
    /// </summary>
    protected async Task CreateTestSubscriptionAsync(string topicName, string subName, string? forwardTo = null)
    {
        var options = new CreateSubscriptionOptions(topicName, subName);
        if (forwardTo is not null)
            options.ForwardTo = forwardTo;
        await AdminClient.CreateSubscriptionAsync(options);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 1: PeekLock Settlement
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PeekLock_Complete_RemovesMessage()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("complete-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("complete-me", msg.Body.ToString());

        // Complete should succeed
        await receiver.CompleteMessageAsync(msg);

        // No more messages should be available
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);
    }

    [Fact]
    public async Task PeekLock_Abandon_RedeliversMessage()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("abandon-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("abandon-me", msg.Body.ToString());
        Assert.Equal(1, msg.DeliveryCount);

        // Abandon the message
        await receiver.AbandonMessageAsync(msg);

        // Message should be re-delivered with incremented delivery count
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("abandon-me", redelivered.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);

        // Clean up
        await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task PeekLock_DeadLetter_MovesToDlq()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("deadletter-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Dead-letter the message
        await receiver.DeadLetterMessageAsync(msg, "TestReason", "Test error description");

        // Original queue should be empty
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);

        // Message should appear in the DLQ
        await using var dlqReceiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            SubQueue = SubQueue.DeadLetter
        });

        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("deadletter-me", dlqMsg.Body.ToString());
        Assert.Equal("TestReason", dlqMsg.DeadLetterReason);
        Assert.Equal("Test error description", dlqMsg.DeadLetterErrorDescription);

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 2: Lock Behavior
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LockExpiry_CompleteFails_MessageRedelivered()
    {
        ThrowIfSkipped();
        // Create queue with a very short lock duration
        var options = new CreateQueueOptions($"placeholder")
        {
            LockDuration = TimeSpan.FromSeconds(5)
        };
        var queue = await CreateTestQueueAsync(options);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("lock-test"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Wait for the lock to expire
        await Task.Delay(TimeSpan.FromSeconds(6));

        // Try to complete after lock expiry.
        // Real Azure Service Bus: throws ServiceBusException with MessageLockLost reason.
        // Emulator: the complete is silently accepted but the message is re-enqueued
        // for redelivery (the emulator cannot reject individual AMQP dispositions
        // without detaching the link, so it accepts the disposition and re-enqueues).
        try
        {
            await receiver.CompleteMessageAsync(msg);
            // Emulator path: complete "succeeded" but message was re-enqueued
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
        {
            // Real ASB path: lock-lost exception is expected
        }

        // The message should be re-delivered regardless of which path we took
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("lock-test", redelivered.Body.ToString());

        await receiver.CompleteMessageAsync(redelivered);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 3: Concurrent Message Delivery
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_MaxConcurrentCalls1_ProcessesSequentially()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        // Send 5 messages
        await using var sender = Client.CreateSender(queue);
        for (int i = 0; i < 5; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"seq-{i}"));
        }

        var timings = new ConcurrentBag<(DateTimeOffset Start, DateTimeOffset End, string Body)>();
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += async args =>
        {
            var start = DateTimeOffset.UtcNow;
            // Simulate some work to make timing measurable
            await Task.Delay(100);
            var end = DateTimeOffset.UtcNow;
            timings.Add((start, end, args.Message.Body.ToString()));

            if (timings.Count >= 5)
                allProcessed.TrySetResult();
        };

        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        // Wait for all messages or timeout
        var completed = await Task.WhenAny(allProcessed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.True(allProcessed.Task.IsCompletedSuccessfully, "Not all 5 messages were processed within 30s");
        Assert.Equal(5, timings.Count);

        // Assert no overlap: each message's start should be after the previous one's end
        var sorted = timings.OrderBy(t => t.Start).ToList();
        for (int i = 1; i < sorted.Count; i++)
        {
            Assert.True(sorted[i].Start >= sorted[i - 1].End,
                $"Message {i} started at {sorted[i].Start:O} but message {i - 1} ended at {sorted[i - 1].End:O} — overlap detected");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 4: Drain/Shutdown
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_Shutdown_CompletesWithinFiveSeconds()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("drain-test"));

        var messageReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += args =>
        {
            messageReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        // Wait for the message to be processed
        var received = await Task.WhenAny(messageReceived.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.True(messageReceived.Task.IsCompletedSuccessfully, "Message was not received within 10s");

        // Stop the processor and measure time
        var sw = Stopwatch.StartNew();
        await processor.StopProcessingAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Processor stop took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 5: Multiple Messages Sequential Processing
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_ThreeMessages_AllReceivedNoDuplicates()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        var sentBodies = new[] { "msg-alpha", "msg-beta", "msg-gamma" };
        await using var sender = Client.CreateSender(queue);
        foreach (var body in sentBodies)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }

        var receivedBodies = new ConcurrentBag<string>();
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += args =>
        {
            receivedBodies.Add(args.Message.Body.ToString());
            if (receivedBodies.Count >= 3)
                allProcessed.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        var completed = await Task.WhenAny(allProcessed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.True(allProcessed.Task.IsCompletedSuccessfully, "Not all 3 messages were processed within 30s");

        // Assert all messages received, no duplicates, no lost messages
        Assert.Equal(3, receivedBodies.Count);
        Assert.Equal(3, receivedBodies.Distinct().Count()); // No duplicates

        foreach (var body in sentBodies)
        {
            Assert.Contains(body, receivedBodies);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 6: Topic Fan-Out
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TopicFanOut_TwoSubscriptions_BothReceiveMessage()
    {
        ThrowIfSkipped();
        var topic = await CreateTestTopicAsync();
        var queue1 = await CreateTestQueueAsync();
        var queue2 = await CreateTestQueueAsync();

        // Create two subscriptions, each forwarding to a different queue
        await CreateTestSubscriptionAsync(topic, "sub-1", forwardTo: queue1);
        await CreateTestSubscriptionAsync(topic, "sub-2", forwardTo: queue2);

        // Publish a message to the topic
        await using var sender = Client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("fan-out-test"));

        // Receive from both queues
        await using var receiver1 = Client.CreateReceiver(queue1, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });
        await using var receiver2 = Client.CreateReceiver(queue2, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg1 = await receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msg2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(msg1);
        Assert.NotNull(msg2);
        Assert.Equal("fan-out-test", msg1.Body.ToString());
        Assert.Equal("fan-out-test", msg2.Body.ToString());

        await receiver1.CompleteMessageAsync(msg1);
        await receiver2.CompleteMessageAsync(msg2);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 7: Message Properties Round-Trip
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MessageProperties_RoundTrip_AllPreserved()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        var outgoing = new ServiceBusMessage("properties-body")
        {
            CorrelationId = "corr-abc-123",
            Subject = "test-subject",
            ContentType = "application/json",
        };
        outgoing.ApplicationProperties["custom-string"] = "hello";
        outgoing.ApplicationProperties["custom-int"] = 42;
        outgoing.ApplicationProperties["custom-bool"] = true;

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(outgoing);

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        Assert.Equal("properties-body", msg.Body.ToString());
        Assert.Equal("corr-abc-123", msg.CorrelationId);
        Assert.Equal("test-subject", msg.Subject);
        Assert.Equal("application/json", msg.ContentType);

        Assert.Equal("hello", msg.ApplicationProperties["custom-string"]);
        Assert.Equal(42, msg.ApplicationProperties["custom-int"]);
        Assert.Equal(true, msg.ApplicationProperties["custom-bool"]);

        await receiver.CompleteMessageAsync(msg);
    }
}
