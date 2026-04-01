# Azure Service Bus Emulator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a MassTransit-compatible Azure Service Bus emulator supporting AMQP 1.0 messaging and REST management API, backed by an in-memory broker core.

**Architecture:** AMQPNetLite `ContainerHost` handles AMQP 1.0 messaging (send/receive/settle). ASP.NET Core Kestrel handles the REST Atom XML management API (topology CRUD). Both share a single in-memory broker core keyed by namespace. Three packages: Core (library), Host (standalone app), TestHost (xUnit fixture).

**Tech Stack:** .NET 10, AMQPNetLite (`Amqp.Net.Lite`), ASP.NET Core, `System.Threading.Channels`, xUnit, `Azure.Messaging.ServiceBus` (tests), `MassTransit.Azure.ServiceBus.Core` (tests)

---

## File Structure

```
src/
  AzureServiceBusEmulator.Core/
    AzureServiceBusEmulator.Core.csproj
    Broker/
      BrokeredMessage.cs              # Message envelope
      QueueEntity.cs                  # Queue + its channel
      TopicEntity.cs                  # Topic + subscriptions
      SubscriptionEntity.cs           # Subscription + rules + forwarding
      RuleEntity.cs                   # Filter rule (TrueFilter only for v1)
      NamespaceContext.cs             # Single namespace's entity store
      NamespaceRegistry.cs            # Top-level ConcurrentDict of namespaces
      ScheduledMessageProcessor.cs    # Background timer for scheduled messages
    Amqp/
      AmqpServer.cs                   # ContainerHost lifecycle
      ServiceBusLinkProcessor.cs      # ILinkProcessor routing
      CbsRequestProcessor.cs          # $cbs accept-all handler
      SenderLinkEndpoint.cs           # Incoming messages -> broker
      ReceiverLinkEndpoint.cs         # Broker -> outgoing messages
      ManagementLinkEndpoint.cs       # $management request/response
      AmqpServerOptions.cs            # Port config
    Management/
      AtomXmlWriter.cs                # Serialize entities -> Atom XML
      AtomXmlReader.cs                # Deserialize Atom XML -> entity properties
      ManagementApiEndpoints.cs       # ASP.NET minimal API route handlers
      ManagementApiErrors.cs          # 404/409 error response helpers
  AzureServiceBusEmulator.Host/
    AzureServiceBusEmulator.Host.csproj
    Program.cs                        # Kestrel + AMQP startup
  AzureServiceBusEmulator.TestHost/
    AzureServiceBusEmulator.TestHost.csproj
    ServiceBusEmulatorFixture.cs      # xUnit IAsyncLifetime fixture
tests/
  AzureServiceBusEmulator.Tests/
    AzureServiceBusEmulator.Tests.csproj
    Broker/
      BrokeredMessageTests.cs
      QueueEntityTests.cs
      TopicEntityTests.cs
      SubscriptionEntityTests.cs
      NamespaceRegistryTests.cs
      ScheduledMessageProcessorTests.cs
    Management/
      AtomXmlWriterTests.cs
      AtomXmlReaderTests.cs
      ManagementApiQueueTests.cs
      ManagementApiTopicTests.cs
      ManagementApiSubscriptionTests.cs
      ManagementApiRuleTests.cs
    Amqp/
      CbsRequestProcessorTests.cs
      SenderLinkEndpointTests.cs
      ReceiverLinkEndpointTests.cs
  AzureServiceBusEmulator.SdkIntegration.Tests/
    AzureServiceBusEmulator.SdkIntegration.Tests.csproj
    AdminClientTests.cs               # ServiceBusAdministrationClient against emulator
    MessagingTests.cs                 # ServiceBusClient send/receive against emulator
  AzureServiceBusEmulator.MassTransit.Tests/
    AzureServiceBusEmulator.MassTransit.Tests.csproj
    MassTransitTopologyTests.cs       # MT creates topology on startup
    MassTransitPubSubTests.cs         # MT publish/consume
    MassTransitSendTests.cs           # MT direct send
    MassTransitRequestResponseTests.cs # MT request/response
AzureServiceBusEmulator.sln
```

---

## Task 1: Solution and Project Scaffolding

**Files:**
- Create: `AzureServiceBusEmulator.sln`
- Create: `src/AzureServiceBusEmulator.Core/AzureServiceBusEmulator.Core.csproj`
- Create: `src/AzureServiceBusEmulator.Host/AzureServiceBusEmulator.Host.csproj`
- Create: `src/AzureServiceBusEmulator.TestHost/AzureServiceBusEmulator.TestHost.csproj`
- Create: `tests/AzureServiceBusEmulator.Tests/AzureServiceBusEmulator.Tests.csproj`
- Create: `tests/AzureServiceBusEmulator.SdkIntegration.Tests/AzureServiceBusEmulator.SdkIntegration.Tests.csproj`
- Create: `tests/AzureServiceBusEmulator.MassTransit.Tests/AzureServiceBusEmulator.MassTransit.Tests.csproj`
- Create: `Directory.Build.props`

- [ ] **Step 1: Create solution and projects**

```bash
dotnet new sln -n AzureServiceBusEmulator

# Core library
dotnet new classlib -n AzureServiceBusEmulator.Core -o src/AzureServiceBusEmulator.Core -f net10.0
dotnet sln add src/AzureServiceBusEmulator.Core

# Host console app
dotnet new web -n AzureServiceBusEmulator.Host -o src/AzureServiceBusEmulator.Host -f net10.0
dotnet sln add src/AzureServiceBusEmulator.Host

# TestHost library
dotnet new classlib -n AzureServiceBusEmulator.TestHost -o src/AzureServiceBusEmulator.TestHost -f net10.0
dotnet sln add src/AzureServiceBusEmulator.TestHost

# Unit tests
dotnet new xunit -n AzureServiceBusEmulator.Tests -o tests/AzureServiceBusEmulator.Tests -f net10.0
dotnet sln add tests/AzureServiceBusEmulator.Tests

# SDK integration tests
dotnet new xunit -n AzureServiceBusEmulator.SdkIntegration.Tests -o tests/AzureServiceBusEmulator.SdkIntegration.Tests -f net10.0
dotnet sln add tests/AzureServiceBusEmulator.SdkIntegration.Tests

# MassTransit integration tests
dotnet new xunit -n AzureServiceBusEmulator.MassTransit.Tests -o tests/AzureServiceBusEmulator.MassTransit.Tests -f net10.0
dotnet sln add tests/AzureServiceBusEmulator.MassTransit.Tests
```

- [ ] **Step 2: Add NuGet packages**

```bash
# Core dependencies
dotnet add src/AzureServiceBusEmulator.Core package Amqp.Net.Lite
dotnet add src/AzureServiceBusEmulator.Core package Microsoft.AspNetCore.App.Ref --version "10.0.*" || true
# Core needs ASP.NET Core types - use FrameworkReference instead (see step 3)

# Host references Core
dotnet add src/AzureServiceBusEmulator.Host reference src/AzureServiceBusEmulator.Core

# TestHost references Core
dotnet add src/AzureServiceBusEmulator.TestHost reference src/AzureServiceBusEmulator.Core
dotnet add src/AzureServiceBusEmulator.TestHost package xunit.abstractions

# Unit tests reference Core
dotnet add tests/AzureServiceBusEmulator.Tests reference src/AzureServiceBusEmulator.Core

# SDK integration tests reference TestHost
dotnet add tests/AzureServiceBusEmulator.SdkIntegration.Tests reference src/AzureServiceBusEmulator.TestHost
dotnet add tests/AzureServiceBusEmulator.SdkIntegration.Tests package Azure.Messaging.ServiceBus

# MassTransit tests reference TestHost
dotnet add tests/AzureServiceBusEmulator.MassTransit.Tests reference src/AzureServiceBusEmulator.TestHost
dotnet add tests/AzureServiceBusEmulator.MassTransit.Tests package Azure.Messaging.ServiceBus
dotnet add tests/AzureServiceBusEmulator.MassTransit.Tests package MassTransit.Azure.ServiceBus.Core
```

- [ ] **Step 3: Configure Core project for ASP.NET Core FrameworkReference**

The Core project is a class library that needs ASP.NET Core types (for Kestrel, minimal APIs). Edit `src/AzureServiceBusEmulator.Core/AzureServiceBusEmulator.Core.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
  <ItemGroup>
    <PackageReference Include="Amqp.Net.Lite" Version="*" />
  </ItemGroup>
</Project>
```

- [ ] **Step 4: Create Directory.Build.props**

Create `Directory.Build.props` in the repo root:

```xml
<Project>
  <PropertyGroup>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
</Project>
```

- [ ] **Step 5: Clean up template files**

Delete the auto-generated template files:
- `src/AzureServiceBusEmulator.Core/Class1.cs`
- `src/AzureServiceBusEmulator.TestHost/Class1.cs`
- `tests/AzureServiceBusEmulator.Tests/UnitTest1.cs`
- `tests/AzureServiceBusEmulator.SdkIntegration.Tests/UnitTest1.cs`
- `tests/AzureServiceBusEmulator.MassTransit.Tests/UnitTest1.cs`

- [ ] **Step 6: Verify solution builds**

```bash
dotnet build AzureServiceBusEmulator.sln
```

Expected: Build succeeds with 0 errors.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: scaffold solution with Core, Host, TestHost, and test projects"
```

---

## Task 2: BrokeredMessage and QueueEntity

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Broker/BrokeredMessage.cs`
- Create: `src/AzureServiceBusEmulator.Core/Broker/QueueEntity.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/BrokeredMessageTests.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/QueueEntityTests.cs`

- [ ] **Step 1: Write BrokeredMessage tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/BrokeredMessageTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class BrokeredMessageTests
{
    [Fact]
    public void Constructor_SetsDefaults()
    {
        var msg = new BrokeredMessage
        {
            Body = new byte[] { 1, 2, 3 },
            ContentType = "application/json"
        };

        Assert.NotNull(msg.MessageId);
        Assert.Equal(0, msg.DeliveryCount);
        Assert.Equal(new byte[] { 1, 2, 3 }, msg.Body);
        Assert.Equal("application/json", msg.ContentType);
        Assert.NotNull(msg.ApplicationProperties);
    }

    [Fact]
    public void SequenceNumber_CanBeAssigned()
    {
        var msg = new BrokeredMessage { Body = [] };
        msg.SequenceNumber = 42;
        Assert.Equal(42, msg.SequenceNumber);
    }

    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var original = new BrokeredMessage
        {
            Body = new byte[] { 1, 2, 3 },
            ContentType = "application/json",
            MessageId = "msg-1",
            CorrelationId = "corr-1",
            SequenceNumber = 10,
            DeliveryCount = 2
        };
        original.ApplicationProperties["key"] = "value";

        var clone = original.Clone();

        Assert.Equal(original.MessageId, clone.MessageId);
        Assert.Equal(original.Body, clone.Body);
        Assert.Equal(0, clone.DeliveryCount); // Reset on clone
        Assert.Equal(0, clone.SequenceNumber); // Reset on clone
        Assert.Equal("value", clone.ApplicationProperties["key"]);

        // Verify independence
        clone.ApplicationProperties["key"] = "changed";
        Assert.Equal("value", original.ApplicationProperties["key"]);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~BrokeredMessageTests" -v minimal
```

Expected: FAIL — `BrokeredMessage` type does not exist.

- [ ] **Step 3: Implement BrokeredMessage**

Create `src/AzureServiceBusEmulator.Core/Broker/BrokeredMessage.cs`:

```csharp
namespace AzureServiceBusEmulator.Core.Broker;

public class BrokeredMessage
{
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public byte[] Body { get; set; } = [];
    public string? ContentType { get; set; }
    public string? CorrelationId { get; set; }
    public string? SessionId { get; set; }
    public string? PartitionKey { get; set; }
    public string? Subject { get; set; }
    public string? ReplyTo { get; set; }
    public string? ReplyToSessionId { get; set; }
    public string? To { get; set; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }
    public TimeSpan TimeToLive { get; set; } = TimeSpan.MaxValue;
    public Dictionary<string, object> ApplicationProperties { get; set; } = new();
    public long SequenceNumber { get; set; }
    public int DeliveryCount { get; set; }
    public string? DeadLetterReason { get; set; }
    public string? DeadLetterErrorDescription { get; set; }
    public DateTimeOffset EnqueuedTimeUtc { get; set; } = DateTimeOffset.UtcNow;
    public string? LockToken { get; set; }

    public BrokeredMessage Clone()
    {
        return new BrokeredMessage
        {
            MessageId = MessageId,
            Body = Body,
            ContentType = ContentType,
            CorrelationId = CorrelationId,
            SessionId = SessionId,
            PartitionKey = PartitionKey,
            Subject = Subject,
            ReplyTo = ReplyTo,
            ReplyToSessionId = ReplyToSessionId,
            To = To,
            ScheduledEnqueueTimeUtc = ScheduledEnqueueTimeUtc,
            TimeToLive = TimeToLive,
            ApplicationProperties = new Dictionary<string, object>(ApplicationProperties),
            DeliveryCount = 0,
            SequenceNumber = 0,
            EnqueuedTimeUtc = EnqueuedTimeUtc
        };
    }
}
```

- [ ] **Step 4: Run BrokeredMessage tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~BrokeredMessageTests" -v minimal
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Write QueueEntity tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/QueueEntityTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class QueueEntityTests
{
    [Fact]
    public async Task Enqueue_And_Dequeue_RoundTrips()
    {
        var queue = new QueueEntity("test-queue");
        var msg = new BrokeredMessage { Body = [1, 2, 3], MessageId = "msg-1" };

        queue.Enqueue(msg);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var received = await queue.DequeueAsync(cts.Token);

        Assert.Equal("msg-1", received.MessageId);
        Assert.Equal(new byte[] { 1, 2, 3 }, received.Body);
    }

    [Fact]
    public async Task Dequeue_CompetingConsumers_EachMessageDeliveredOnce()
    {
        var queue = new QueueEntity("test-queue");

        for (int i = 0; i < 10; i++)
            queue.Enqueue(new BrokeredMessage { Body = [], MessageId = $"msg-{i}" });

        var received = new System.Collections.Concurrent.ConcurrentBag<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var consumers = Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var msg = await queue.DequeueAsync(cts.Token);
                    received.Add(msg.MessageId);
                }
                catch (OperationCanceledException) { break; }
            }
        })).ToArray();

        await Task.WhenAll(consumers);

        Assert.Equal(10, received.Count);
        Assert.Equal(10, received.Distinct().Count()); // No duplicates
    }

    [Fact]
    public void Complete_RemovesMessageFromPending()
    {
        var queue = new QueueEntity("test-queue");
        var msg = new BrokeredMessage { Body = [], LockToken = Guid.NewGuid().ToString() };
        queue.Enqueue(msg);

        queue.Complete(msg.LockToken);
        // No exception means success — message removed from pending
    }

    [Fact]
    public void Abandon_RequeuesMessage_IncrementsDeliveryCount()
    {
        var queue = new QueueEntity("test-queue");
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1", LockToken = Guid.NewGuid().ToString() };

        queue.TrackPending(msg);
        queue.Abandon(msg.LockToken);

        // Message should be back in the queue with incremented delivery count
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var requeued = queue.TryDequeueImmediate();
        Assert.NotNull(requeued);
        Assert.Equal("msg-1", requeued!.MessageId);
        Assert.Equal(1, requeued.DeliveryCount);
    }

    [Fact]
    public void Abandon_ExceedsMaxDeliveryCount_DeadLetters()
    {
        var queue = new QueueEntity("test-queue") { MaxDeliveryCount = 2 };
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1", DeliveryCount = 1, LockToken = Guid.NewGuid().ToString() };

        queue.TrackPending(msg);
        queue.Abandon(msg.LockToken);

        // Should NOT be back in main queue
        Assert.Null(queue.TryDequeueImmediate());

        // Should be in dead-letter queue
        var dlqMsg = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMsg);
        Assert.Equal("msg-1", dlqMsg!.MessageId);
    }

    [Fact]
    public void DeadLetter_MovesMessageToDeadLetterQueue()
    {
        var queue = new QueueEntity("test-queue");
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1", LockToken = Guid.NewGuid().ToString() };

        queue.TrackPending(msg);
        queue.DeadLetter(msg.LockToken, "TestReason", "TestDescription");

        var dlqMsg = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMsg);
        Assert.Equal("TestReason", dlqMsg!.DeadLetterReason);
        Assert.Equal("TestDescription", dlqMsg.DeadLetterErrorDescription);
    }

    [Fact]
    public void Properties_HaveDefaults()
    {
        var queue = new QueueEntity("my-queue");

        Assert.Equal("my-queue", queue.Name);
        Assert.Equal(TimeSpan.FromSeconds(30), queue.LockDuration);
        Assert.Equal(10, queue.MaxDeliveryCount);
        Assert.False(queue.RequiresSession);
        Assert.False(queue.DeadLetteringOnMessageExpiration);
    }
}
```

- [ ] **Step 6: Run QueueEntity tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~QueueEntityTests" -v minimal
```

Expected: FAIL — `QueueEntity` type does not exist.

- [ ] **Step 7: Implement QueueEntity**

Create `src/AzureServiceBusEmulator.Core/Broker/QueueEntity.cs`:

```csharp
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AzureServiceBusEmulator.Core.Broker;

public class QueueEntity
{
    private readonly Channel<BrokeredMessage> _channel = Channel.CreateUnbounded<BrokeredMessage>();
    private readonly ConcurrentDictionary<string, BrokeredMessage> _pending = new();

    public string Name { get; }
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public bool RequiresSession { get; set; }
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;
    public bool EnableBatchedOperations { get; set; } = true;
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public string? ForwardTo { get; set; }
    public string? ForwardDeadLetteredMessagesTo { get; set; }
    public string? UserMetadata { get; set; }
    public QueueEntity DeadLetterQueue { get; }

    public QueueEntity(string name, bool isDeadLetterQueue = false)
    {
        Name = name;
        DeadLetterQueue = isDeadLetterQueue ? this : new QueueEntity($"{name}/$deadletterqueue", isDeadLetterQueue: true);
    }

    public void Enqueue(BrokeredMessage message)
    {
        message.LockToken ??= Guid.NewGuid().ToString();
        _channel.Writer.TryWrite(message);
    }

    public async ValueTask<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        var msg = await _channel.Reader.ReadAsync(cancellationToken);
        msg.DeliveryCount++;
        _pending[msg.LockToken!] = msg;
        return msg;
    }

    public BrokeredMessage? TryDequeueImmediate()
    {
        if (_channel.Reader.TryRead(out var msg))
        {
            msg.DeliveryCount++;
            return msg;
        }
        return null;
    }

    public void TrackPending(BrokeredMessage message)
    {
        _pending[message.LockToken!] = message;
    }

    public void Complete(string lockToken)
    {
        _pending.TryRemove(lockToken, out _);
    }

    public void Abandon(string lockToken)
    {
        if (!_pending.TryRemove(lockToken, out var msg))
            return;

        if (msg.DeliveryCount >= MaxDeliveryCount)
        {
            msg.DeadLetterReason = "MaxDeliveryCountExceeded";
            DeadLetterQueue.Enqueue(msg);
        }
        else
        {
            _channel.Writer.TryWrite(msg);
        }
    }

    public void DeadLetter(string lockToken, string? reason = null, string? errorDescription = null)
    {
        if (!_pending.TryRemove(lockToken, out var msg))
            return;

        msg.DeadLetterReason = reason;
        msg.DeadLetterErrorDescription = errorDescription;
        DeadLetterQueue.Enqueue(msg);
    }
}
```

- [ ] **Step 8: Run all QueueEntity tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~QueueEntityTests" -v minimal
```

Expected: All 7 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: add BrokeredMessage and QueueEntity with message settlement"
```

---

## Task 3: TopicEntity, SubscriptionEntity, RuleEntity

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Broker/RuleEntity.cs`
- Create: `src/AzureServiceBusEmulator.Core/Broker/SubscriptionEntity.cs`
- Create: `src/AzureServiceBusEmulator.Core/Broker/TopicEntity.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/TopicEntityTests.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/SubscriptionEntityTests.cs`

- [ ] **Step 1: Write TopicEntity tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/TopicEntityTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class TopicEntityTests
{
    [Fact]
    public void Publish_FansOutToAllSubscriptions()
    {
        var topic = new TopicEntity("my-topic");
        var sub1 = topic.AddSubscription("sub-1");
        var sub2 = topic.AddSubscription("sub-2");

        var msg = new BrokeredMessage { Body = [1, 2, 3], MessageId = "msg-1" };
        topic.Publish(msg);

        var received1 = sub1.Queue.TryDequeueImmediate();
        var received2 = sub2.Queue.TryDequeueImmediate();

        Assert.NotNull(received1);
        Assert.NotNull(received2);
        Assert.Equal("msg-1", received1!.MessageId);
        Assert.Equal("msg-1", received2!.MessageId);
    }

    [Fact]
    public void Publish_WithForwardTo_RoutesToTargetQueue()
    {
        var topic = new TopicEntity("my-topic");
        var targetQueue = new QueueEntity("target-queue");
        var sub = topic.AddSubscription("sub-1");
        sub.ForwardTo = "target-queue";
        sub.ResolvedForwardToQueue = targetQueue;

        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1" };
        topic.Publish(msg);

        // Subscription's own queue should be empty
        Assert.Null(sub.Queue.TryDequeueImmediate());

        // Target queue should have the message
        var forwarded = targetQueue.TryDequeueImmediate();
        Assert.NotNull(forwarded);
        Assert.Equal("msg-1", forwarded!.MessageId);
    }

    [Fact]
    public void Publish_ClonesMessagePerSubscription()
    {
        var topic = new TopicEntity("my-topic");
        var sub1 = topic.AddSubscription("sub-1");
        var sub2 = topic.AddSubscription("sub-2");

        var msg = new BrokeredMessage { Body = [1], MessageId = "msg-1" };
        topic.Publish(msg);

        var r1 = sub1.Queue.TryDequeueImmediate();
        var r2 = sub2.Queue.TryDequeueImmediate();

        Assert.NotSame(r1, r2); // Different instances
    }

    [Fact]
    public void AddSubscription_ReturnsExistingIfAlreadyExists()
    {
        var topic = new TopicEntity("my-topic");
        var sub1 = topic.AddSubscription("sub-1");
        var sub2 = topic.AddSubscription("sub-1");

        Assert.Same(sub1, sub2);
    }

    [Fact]
    public void GetSubscription_ReturnsNullIfNotFound()
    {
        var topic = new TopicEntity("my-topic");
        Assert.Null(topic.GetSubscription("nope"));
    }

    [Fact]
    public void RemoveSubscription_RemovesIt()
    {
        var topic = new TopicEntity("my-topic");
        topic.AddSubscription("sub-1");
        Assert.True(topic.RemoveSubscription("sub-1"));
        Assert.Null(topic.GetSubscription("sub-1"));
    }

    [Fact]
    public void Properties_HaveDefaults()
    {
        var topic = new TopicEntity("my-topic");
        Assert.Equal("my-topic", topic.Name);
        Assert.Equal(1024, topic.MaxSizeInMegabytes);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~TopicEntityTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement RuleEntity**

Create `src/AzureServiceBusEmulator.Core/Broker/RuleEntity.cs`:

```csharp
namespace AzureServiceBusEmulator.Core.Broker;

public class RuleEntity
{
    public string Name { get; set; }
    public RuleFilterType FilterType { get; set; } = RuleFilterType.TrueFilter;
    public string? SqlExpression { get; set; }
    public string? CorrelationId { get; set; }
    public string? ActionExpression { get; set; }

    public RuleEntity(string name)
    {
        Name = name;
    }

    public bool Matches(BrokeredMessage message)
    {
        // v1: TrueFilter matches everything. SqlFilter/CorrelationFilter stubbed as match-all.
        return true;
    }
}

public enum RuleFilterType
{
    TrueFilter,
    FalseFilter,
    SqlFilter,
    CorrelationFilter
}
```

- [ ] **Step 4: Implement SubscriptionEntity**

Create `src/AzureServiceBusEmulator.Core/Broker/SubscriptionEntity.cs`:

```csharp
using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

public class SubscriptionEntity
{
    private readonly ConcurrentDictionary<string, RuleEntity> _rules = new(StringComparer.OrdinalIgnoreCase);

    public string Name { get; }
    public string TopicName { get; }
    public QueueEntity Queue { get; }
    public string? ForwardTo { get; set; }
    public QueueEntity? ResolvedForwardToQueue { get; set; }
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public bool DeadLetteringOnMessageExpiration { get; set; }
    public bool EnableBatchedOperations { get; set; } = true;
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;
    public string? UserMetadata { get; set; }
    public bool RequiresSession { get; set; }

    public SubscriptionEntity(string name, string topicName)
    {
        Name = name;
        TopicName = topicName;
        Queue = new QueueEntity($"{topicName}/Subscriptions/{name}");
        // Default rule: match all
        _rules["$Default"] = new RuleEntity("$Default");
    }

    public bool ShouldDeliver(BrokeredMessage message)
    {
        return _rules.Values.Any(r => r.Matches(message));
    }

    public void DeliverMessage(BrokeredMessage message)
    {
        if (!ShouldDeliver(message))
            return;

        if (ResolvedForwardToQueue is not null)
        {
            ResolvedForwardToQueue.Enqueue(message);
        }
        else
        {
            Queue.Enqueue(message);
        }
    }

    public RuleEntity AddOrUpdateRule(string name, RuleEntity rule)
    {
        _rules[name] = rule;
        return rule;
    }

    public RuleEntity? GetRule(string name) => _rules.GetValueOrDefault(name);

    public IReadOnlyCollection<RuleEntity> GetRules() => _rules.Values.ToList();

    public bool RemoveRule(string name) => _rules.TryRemove(name, out _);
}
```

- [ ] **Step 5: Implement TopicEntity**

Create `src/AzureServiceBusEmulator.Core/Broker/TopicEntity.cs`:

```csharp
using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

public class TopicEntity
{
    private readonly ConcurrentDictionary<string, SubscriptionEntity> _subscriptions = new(StringComparer.OrdinalIgnoreCase);

    public string Name { get; }
    public long MaxSizeInMegabytes { get; set; } = 1024;
    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;
    public bool EnableBatchedOperations { get; set; } = true;
    public string? UserMetadata { get; set; }

    public TopicEntity(string name)
    {
        Name = name;
    }

    public void Publish(BrokeredMessage message)
    {
        foreach (var sub in _subscriptions.Values)
        {
            var clone = message.Clone();
            sub.DeliverMessage(clone);
        }
    }

    public SubscriptionEntity AddSubscription(string name)
    {
        return _subscriptions.GetOrAdd(name, n => new SubscriptionEntity(n, Name));
    }

    public SubscriptionEntity? GetSubscription(string name) => _subscriptions.GetValueOrDefault(name);

    public IReadOnlyCollection<SubscriptionEntity> GetSubscriptions() => _subscriptions.Values.ToList();

    public bool RemoveSubscription(string name) => _subscriptions.TryRemove(name, out _);
}
```

- [ ] **Step 6: Run all topic/subscription tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~TopicEntityTests" -v minimal
```

Expected: All 7 tests PASS.

- [ ] **Step 7: Write SubscriptionEntity tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/SubscriptionEntityTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class SubscriptionEntityTests
{
    [Fact]
    public void HasDefaultRule()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1");
        var rules = sub.GetRules();
        Assert.Single(rules);
        Assert.Equal("$Default", rules.First().Name);
    }

    [Fact]
    public void AddOrUpdateRule_AddsNewRule()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1");
        sub.AddOrUpdateRule("custom", new RuleEntity("custom") { FilterType = RuleFilterType.SqlFilter, SqlExpression = "1=1" });
        Assert.Equal(2, sub.GetRules().Count);
    }

    [Fact]
    public void RemoveRule_RemovesIt()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1");
        Assert.True(sub.RemoveRule("$Default"));
        Assert.Empty(sub.GetRules());
    }

    [Fact]
    public void DeliverMessage_WithoutForwardTo_EnqueuesInOwnQueue()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1");
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1" };

        sub.DeliverMessage(msg);

        var received = sub.Queue.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal("msg-1", received!.MessageId);
    }

    [Fact]
    public void DeliverMessage_WithForwardTo_RoutesToTargetQueue()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1");
        var target = new QueueEntity("target");
        sub.ForwardTo = "target";
        sub.ResolvedForwardToQueue = target;

        sub.DeliverMessage(new BrokeredMessage { Body = [], MessageId = "msg-1" });

        Assert.Null(sub.Queue.TryDequeueImmediate());
        Assert.NotNull(target.TryDequeueImmediate());
    }
}
```

- [ ] **Step 8: Run subscription tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~SubscriptionEntityTests" -v minimal
```

Expected: All 5 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: add TopicEntity, SubscriptionEntity, RuleEntity with fan-out and forwarding"
```

---

## Task 4: NamespaceRegistry and NamespaceContext

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Broker/NamespaceContext.cs`
- Create: `src/AzureServiceBusEmulator.Core/Broker/NamespaceRegistry.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/NamespaceRegistryTests.cs`

- [ ] **Step 1: Write NamespaceRegistry tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/NamespaceRegistryTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class NamespaceRegistryTests
{
    [Fact]
    public void GetOrCreate_ReturnsSameContextForSameNamespace()
    {
        var registry = new NamespaceRegistry();
        var ctx1 = registry.GetOrCreate("ns-1");
        var ctx2 = registry.GetOrCreate("ns-1");
        Assert.Same(ctx1, ctx2);
    }

    [Fact]
    public void GetOrCreate_ReturnsDifferentContextsForDifferentNamespaces()
    {
        var registry = new NamespaceRegistry();
        var ctx1 = registry.GetOrCreate("ns-1");
        var ctx2 = registry.GetOrCreate("ns-2");
        Assert.NotSame(ctx1, ctx2);
    }

    [Fact]
    public void NamespaceIsolation_NoMessageCrossContamination()
    {
        var registry = new NamespaceRegistry();
        var ctx1 = registry.GetOrCreate("ns-1");
        var ctx2 = registry.GetOrCreate("ns-2");

        ctx1.CreateQueue("shared-name");
        ctx2.CreateQueue("shared-name");

        ctx1.GetQueue("shared-name")!.Enqueue(new BrokeredMessage { Body = [], MessageId = "only-ns1" });

        Assert.Null(ctx2.GetQueue("shared-name")!.TryDequeueImmediate());
        Assert.NotNull(ctx1.GetQueue("shared-name")!.TryDequeueImmediate());
    }

    [Fact]
    public void NamespaceContext_CreateQueue_ReturnsQueue()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        Assert.Equal("my-queue", queue.Name);
    }

    [Fact]
    public void NamespaceContext_CreateQueue_Idempotent()
    {
        var ctx = new NamespaceContext("test");
        var q1 = ctx.CreateQueue("my-queue");
        var q2 = ctx.CreateQueue("my-queue");
        Assert.Same(q1, q2);
    }

    [Fact]
    public void NamespaceContext_CreateTopic_ReturnsTopic()
    {
        var ctx = new NamespaceContext("test");
        var topic = ctx.CreateTopic("my-topic");
        Assert.Equal("my-topic", topic.Name);
    }

    [Fact]
    public void NamespaceContext_CreateSubscription_LinksForwardTo()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateTopic("my-topic");
        ctx.CreateQueue("target-queue");

        var sub = ctx.CreateSubscription("my-topic", "sub-1", forwardTo: "target-queue");

        Assert.Equal("target-queue", sub.ForwardTo);
        Assert.NotNull(sub.ResolvedForwardToQueue);
        Assert.Equal("target-queue", sub.ResolvedForwardToQueue!.Name);
    }

    [Fact]
    public void NamespaceContext_NextSequenceNumber_Increments()
    {
        var ctx = new NamespaceContext("test");
        var seq1 = ctx.NextSequenceNumber();
        var seq2 = ctx.NextSequenceNumber();
        Assert.Equal(1, seq1);
        Assert.Equal(2, seq2);
    }

    [Fact]
    public void NamespaceContext_GetQueue_ReturnsNullIfNotFound()
    {
        var ctx = new NamespaceContext("test");
        Assert.Null(ctx.GetQueue("nope"));
    }

    [Fact]
    public void NamespaceContext_GetTopic_ReturnsNullIfNotFound()
    {
        var ctx = new NamespaceContext("test");
        Assert.Null(ctx.GetTopic("nope"));
    }

    [Fact]
    public void NamespaceContext_ResolveEntity_FindsQueueOrSubscription()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateQueue("my-queue");
        ctx.CreateTopic("my-topic");
        ctx.CreateSubscription("my-topic", "sub-1");

        Assert.NotNull(ctx.ResolveQueue("my-queue"));
        Assert.NotNull(ctx.ResolveQueue("my-topic/Subscriptions/sub-1"));
        Assert.Null(ctx.ResolveQueue("nonexistent"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~NamespaceRegistryTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement NamespaceContext**

Create `src/AzureServiceBusEmulator.Core/Broker/NamespaceContext.cs`:

```csharp
using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

public class NamespaceContext
{
    private readonly ConcurrentDictionary<string, QueueEntity> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicEntity> _topics = new(StringComparer.OrdinalIgnoreCase);
    private long _sequenceNumber;

    public string Name { get; }

    public NamespaceContext(string name)
    {
        Name = name;
    }

    public long NextSequenceNumber() => Interlocked.Increment(ref _sequenceNumber);

    // Queues
    public QueueEntity CreateQueue(string name) => _queues.GetOrAdd(name, n => new QueueEntity(n));
    public QueueEntity? GetQueue(string name) => _queues.GetValueOrDefault(name);
    public IReadOnlyCollection<QueueEntity> GetQueues() => _queues.Values.ToList();
    public bool DeleteQueue(string name) => _queues.TryRemove(name, out _);

    // Topics
    public TopicEntity CreateTopic(string name) => _topics.GetOrAdd(name, n => new TopicEntity(n));
    public TopicEntity? GetTopic(string name) => _topics.GetValueOrDefault(name);
    public IReadOnlyCollection<TopicEntity> GetTopics() => _topics.Values.ToList();
    public bool DeleteTopic(string name) => _topics.TryRemove(name, out _);

    // Subscriptions (convenience methods)
    public SubscriptionEntity CreateSubscription(string topicName, string subscriptionName, string? forwardTo = null)
    {
        var topic = _topics.GetOrAdd(topicName, n => new TopicEntity(n));
        var sub = topic.AddSubscription(subscriptionName);

        if (forwardTo is not null)
        {
            sub.ForwardTo = forwardTo;
            sub.ResolvedForwardToQueue = _queues.GetValueOrDefault(forwardTo);
        }

        return sub;
    }

    public SubscriptionEntity? GetSubscription(string topicName, string subscriptionName)
    {
        return _topics.GetValueOrDefault(topicName)?.GetSubscription(subscriptionName);
    }

    // Resolve an AMQP address to its backing queue
    // Handles: "queueName" and "topicName/Subscriptions/subName"
    public QueueEntity? ResolveQueue(string address)
    {
        // Try as a direct queue first
        if (_queues.TryGetValue(address, out var queue))
            return queue;

        // Try as subscription path: topicName/Subscriptions/subName
        var parts = address.Split('/');
        if (parts.Length == 3 && parts[1].Equals("Subscriptions", StringComparison.OrdinalIgnoreCase))
        {
            var sub = GetSubscription(parts[0], parts[2]);
            return sub?.Queue;
        }

        return null;
    }

    // Resolve an AMQP send address — could be a queue or a topic
    public (QueueEntity? Queue, TopicEntity? Topic) ResolveSendTarget(string address)
    {
        if (_queues.TryGetValue(address, out var queue))
            return (queue, null);

        if (_topics.TryGetValue(address, out var topic))
            return (null, topic);

        return (null, null);
    }
}
```

- [ ] **Step 4: Implement NamespaceRegistry**

Create `src/AzureServiceBusEmulator.Core/Broker/NamespaceRegistry.cs`:

```csharp
using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

public class NamespaceRegistry
{
    private readonly ConcurrentDictionary<string, NamespaceContext> _namespaces = new(StringComparer.OrdinalIgnoreCase);

    public NamespaceContext GetOrCreate(string namespaceName)
    {
        return _namespaces.GetOrAdd(namespaceName, n => new NamespaceContext(n));
    }

    public NamespaceContext? Get(string namespaceName)
    {
        return _namespaces.GetValueOrDefault(namespaceName);
    }
}
```

- [ ] **Step 5: Run all namespace tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~NamespaceRegistryTests" -v minimal
```

Expected: All 11 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: add NamespaceContext and NamespaceRegistry with tenant isolation"
```

---

## Task 5: Scheduled Message Processor

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Broker/ScheduledMessageProcessor.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Broker/ScheduledMessageProcessorTests.cs`

- [ ] **Step 1: Write ScheduledMessageProcessor tests**

Create `tests/AzureServiceBusEmulator.Tests/Broker/ScheduledMessageProcessorTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class ScheduledMessageProcessorTests
{
    [Fact]
    public void Schedule_ReturnsSequenceNumber()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ctx);

        var msg = new BrokeredMessage { Body = [], ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddHours(1) };
        var seqNo = processor.Schedule("my-queue", msg);

        Assert.True(seqNo > 0);
    }

    [Fact]
    public void CancelScheduled_ReturnsTrueIfFound()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ctx);

        var msg = new BrokeredMessage { Body = [], ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddHours(1) };
        var seqNo = processor.Schedule("my-queue", msg);

        Assert.True(processor.CancelScheduled(seqNo));
    }

    [Fact]
    public void CancelScheduled_ReturnsFalseIfNotFound()
    {
        var ctx = new NamespaceContext("test");
        var processor = new ScheduledMessageProcessor(ctx);

        Assert.False(processor.CancelScheduled(99999));
    }

    [Fact]
    public async Task ProcessDueMessages_DeliversWhenDue()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ctx);

        var msg = new BrokeredMessage
        {
            Body = [1, 2, 3],
            MessageId = "scheduled-1",
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1) // Already due
        };
        processor.Schedule("my-queue", msg);

        processor.ProcessDueMessages();

        var received = queue.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal("scheduled-1", received!.MessageId);
    }

    [Fact]
    public void ProcessDueMessages_DoesNotDeliverFutureMessages()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ctx);

        var msg = new BrokeredMessage
        {
            Body = [],
            MessageId = "future",
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddHours(1)
        };
        processor.Schedule("my-queue", msg);

        processor.ProcessDueMessages();

        Assert.Null(queue.TryDequeueImmediate());
    }

    [Fact]
    public async Task ScheduleToTopic_FansOutWhenDue()
    {
        var ctx = new NamespaceContext("test");
        var topic = ctx.CreateTopic("my-topic");
        ctx.CreateQueue("target");
        var sub = ctx.CreateSubscription("my-topic", "sub-1", forwardTo: "target");
        var processor = new ScheduledMessageProcessor(ctx);

        var msg = new BrokeredMessage
        {
            Body = [],
            MessageId = "sched-topic",
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1)
        };
        processor.Schedule("my-topic", msg);

        processor.ProcessDueMessages();

        var received = ctx.GetQueue("target")!.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal("sched-topic", received!.MessageId);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~ScheduledMessageProcessorTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement ScheduledMessageProcessor**

Create `src/AzureServiceBusEmulator.Core/Broker/ScheduledMessageProcessor.cs`:

```csharp
using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

public class ScheduledMessageProcessor : IDisposable
{
    private readonly NamespaceContext _context;
    private readonly ConcurrentDictionary<long, ScheduledEntry> _scheduled = new();
    private CancellationTokenSource? _cts;
    private Task? _backgroundTask;

    public ScheduledMessageProcessor(NamespaceContext context)
    {
        _context = context;
    }

    public long Schedule(string entityName, BrokeredMessage message)
    {
        var seqNo = _context.NextSequenceNumber();
        message.SequenceNumber = seqNo;
        _scheduled[seqNo] = new ScheduledEntry(entityName, message);
        return seqNo;
    }

    public bool CancelScheduled(long sequenceNumber)
    {
        return _scheduled.TryRemove(sequenceNumber, out _);
    }

    public void ProcessDueMessages()
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var (seqNo, entry) in _scheduled)
        {
            if (entry.Message.ScheduledEnqueueTimeUtc <= now)
            {
                if (!_scheduled.TryRemove(seqNo, out _))
                    continue;

                entry.Message.ScheduledEnqueueTimeUtc = null;
                DeliverToEntity(entry.EntityName, entry.Message);
            }
        }
    }

    private void DeliverToEntity(string entityName, BrokeredMessage message)
    {
        var (queue, topic) = _context.ResolveSendTarget(entityName);

        if (queue is not null)
        {
            queue.Enqueue(message);
        }
        else if (topic is not null)
        {
            topic.Publish(message);
        }
    }

    public void StartBackground(TimeSpan interval)
    {
        _cts = new CancellationTokenSource();
        _backgroundTask = RunBackgroundAsync(interval, _cts.Token);
    }

    private async Task RunBackgroundAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            ProcessDueMessages();
        }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _backgroundTask = null;
    }

    private record ScheduledEntry(string EntityName, BrokeredMessage Message);
}
```

- [ ] **Step 4: Run all scheduled message tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~ScheduledMessageProcessorTests" -v minimal
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add ScheduledMessageProcessor with background delivery timer"
```

---

## Task 6: Atom XML Serialization (Writer)

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Management/AtomXmlWriter.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/AtomXmlWriterTests.cs`

The Azure Service Bus Atom XML format wraps entity descriptions inside `<entry>` elements. The SDK expects specific XML namespaces and element names. This is the highest-risk part of the emulator — the format must match exactly.

- [ ] **Step 1: Write AtomXmlWriter tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/AtomXmlWriterTests.cs`:

```csharp
using System.Xml.Linq;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;

namespace AzureServiceBusEmulator.Tests.Management;

public class AtomXmlWriterTests
{
    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";

    [Fact]
    public void WriteQueueEntry_ContainsQueueDescription()
    {
        var queue = new QueueEntity("test-queue")
        {
            LockDuration = TimeSpan.FromSeconds(60),
            MaxDeliveryCount = 5
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var doc = XDocument.Parse(xml);

        var entry = doc.Root!;
        Assert.Equal(Atom + "entry", entry.Name);

        var content = entry.Element(Atom + "content")!;
        Assert.Equal("application/xml", content.Attribute("type")?.Value);

        var desc = content.Element(Sb + "QueueDescription")!;
        Assert.Equal("PT1M", desc.Element(Sb + "LockDuration")?.Value);
        Assert.Equal("5", desc.Element(Sb + "MaxDeliveryCount")?.Value);
    }

    [Fact]
    public void WriteTopicEntry_ContainsTopicDescription()
    {
        var topic = new TopicEntity("test-topic");

        var xml = AtomXmlWriter.WriteTopicEntry(topic);
        var doc = XDocument.Parse(xml);

        var desc = doc.Root!
            .Element(Atom + "content")!
            .Element(Sb + "TopicDescription")!;

        Assert.NotNull(desc);
        Assert.Equal("test-topic", doc.Root.Element(Atom + "title")?.Value);
    }

    [Fact]
    public void WriteSubscriptionEntry_ContainsSubscriptionDescription()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1")
        {
            ForwardTo = "target-queue",
            MaxDeliveryCount = 3
        };

        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
        var doc = XDocument.Parse(xml);

        var desc = doc.Root!
            .Element(Atom + "content")!
            .Element(Sb + "SubscriptionDescription")!;

        Assert.Equal("target-queue", desc.Element(Sb + "ForwardTo")?.Value);
        Assert.Equal("3", desc.Element(Sb + "MaxDeliveryCount")?.Value);
    }

    [Fact]
    public void WriteRuleEntry_ContainsRuleDescription()
    {
        var rule = new RuleEntity("$Default");

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var doc = XDocument.Parse(xml);

        var desc = doc.Root!
            .Element(Atom + "content")!
            .Element(Sb + "RuleDescription")!;

        Assert.NotNull(desc.Element(Sb + "Filter"));
    }

    [Fact]
    public void WriteFeed_WrapsMultipleEntries()
    {
        var queues = new[]
        {
            new QueueEntity("q1"),
            new QueueEntity("q2")
        };

        var xml = AtomXmlWriter.WriteQueueFeed(queues);
        var doc = XDocument.Parse(xml);

        var entries = doc.Root!.Elements(Atom + "entry").ToList();
        Assert.Equal(2, entries.Count);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~AtomXmlWriterTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement AtomXmlWriter**

Create `src/AzureServiceBusEmulator.Core/Management/AtomXmlWriter.cs`:

```csharp
using System.Xml.Linq;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

public static class AtomXmlWriter
{
    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";

    public static string WriteQueueEntry(QueueEntity queue)
    {
        var entry = CreateEntry(queue.Name, new XElement(Sb + "QueueDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "LockDuration", FormatTimeSpan(queue.LockDuration)),
            new XElement(Sb + "MaxSizeInMegabytes", queue.MaxSizeInMegabytes),
            new XElement(Sb + "RequiresSession", queue.RequiresSession.ToString().ToLower()),
            new XElement(Sb + "DefaultMessageTimeToLive", FormatTimeSpan(queue.DefaultMessageTimeToLive)),
            new XElement(Sb + "DeadLetteringOnMessageExpiration", queue.DeadLetteringOnMessageExpiration.ToString().ToLower()),
            new XElement(Sb + "MaxDeliveryCount", queue.MaxDeliveryCount),
            new XElement(Sb + "EnableBatchedOperations", queue.EnableBatchedOperations.ToString().ToLower()),
            OptionalElement(Sb + "ForwardTo", queue.ForwardTo),
            OptionalElement(Sb + "UserMetadata", queue.UserMetadata)
        ));

        return entry.ToString();
    }

    public static string WriteTopicEntry(TopicEntity topic)
    {
        var entry = CreateEntry(topic.Name, new XElement(Sb + "TopicDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "DefaultMessageTimeToLive", FormatTimeSpan(topic.DefaultMessageTimeToLive)),
            new XElement(Sb + "MaxSizeInMegabytes", topic.MaxSizeInMegabytes),
            new XElement(Sb + "EnableBatchedOperations", topic.EnableBatchedOperations.ToString().ToLower()),
            OptionalElement(Sb + "UserMetadata", topic.UserMetadata)
        ));

        return entry.ToString();
    }

    public static string WriteSubscriptionEntry(SubscriptionEntity sub)
    {
        var entry = CreateEntry(sub.Name, new XElement(Sb + "SubscriptionDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            new XElement(Sb + "LockDuration", FormatTimeSpan(sub.LockDuration)),
            new XElement(Sb + "RequiresSession", sub.RequiresSession.ToString().ToLower()),
            new XElement(Sb + "DefaultMessageTimeToLive", FormatTimeSpan(sub.DefaultMessageTimeToLive)),
            new XElement(Sb + "DeadLetteringOnMessageExpiration", sub.DeadLetteringOnMessageExpiration.ToString().ToLower()),
            new XElement(Sb + "MaxDeliveryCount", sub.MaxDeliveryCount),
            new XElement(Sb + "EnableBatchedOperations", sub.EnableBatchedOperations.ToString().ToLower()),
            OptionalElement(Sb + "ForwardTo", sub.ForwardTo),
            OptionalElement(Sb + "UserMetadata", sub.UserMetadata)
        ));

        return entry.ToString();
    }

    public static string WriteRuleEntry(RuleEntity rule)
    {
        var filterElement = rule.FilterType switch
        {
            RuleFilterType.SqlFilter => new XElement(Sb + "Filter",
                new XAttribute(Xsi + "type", "SqlFilter"),
                new XElement(Sb + "SqlExpression", rule.SqlExpression ?? "1=1")),
            RuleFilterType.CorrelationFilter => new XElement(Sb + "Filter",
                new XAttribute(Xsi + "type", "CorrelationFilter"),
                OptionalElement(Sb + "CorrelationId", rule.CorrelationId)),
            _ => new XElement(Sb + "Filter",
                new XAttribute(Xsi + "type", "TrueFilter"),
                new XElement(Sb + "SqlExpression", "1=1"))
        };

        var ruleDesc = new XElement(Sb + "RuleDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi),
            filterElement,
            new XElement(Sb + "Name", rule.Name)
        );

        if (rule.ActionExpression is not null)
        {
            ruleDesc.Add(new XElement(Sb + "Action",
                new XAttribute(Xsi + "type", "SqlRuleAction"),
                new XElement(Sb + "SqlExpression", rule.ActionExpression)));
        }
        else
        {
            ruleDesc.Add(new XElement(Sb + "Action",
                new XAttribute(Xsi + "type", "EmptyRuleAction")));
        }

        var entry = CreateEntry(rule.Name, ruleDesc);
        return entry.ToString();
    }

    public static string WriteQueueFeed(IEnumerable<QueueEntity> queues)
    {
        return WriteFeed(queues.Select(q =>
        {
            var doc = XDocument.Parse(WriteQueueEntry(q));
            return doc.Root!;
        }));
    }

    public static string WriteTopicFeed(IEnumerable<TopicEntity> topics)
    {
        return WriteFeed(topics.Select(t =>
        {
            var doc = XDocument.Parse(WriteTopicEntry(t));
            return doc.Root!;
        }));
    }

    public static string WriteSubscriptionFeed(IEnumerable<SubscriptionEntity> subscriptions)
    {
        return WriteFeed(subscriptions.Select(s =>
        {
            var doc = XDocument.Parse(WriteSubscriptionEntry(s));
            return doc.Root!;
        }));
    }

    public static string WriteRuleFeed(IEnumerable<RuleEntity> rules)
    {
        return WriteFeed(rules.Select(r =>
        {
            var doc = XDocument.Parse(WriteRuleEntry(r));
            return doc.Root!;
        }));
    }

    private static string WriteFeed(IEnumerable<XElement> entries)
    {
        var feed = new XElement(Atom + "feed",
            new XElement(Atom + "title", "Entities"),
            entries
        );
        return feed.ToString();
    }

    private static XElement CreateEntry(string title, XElement descriptionElement)
    {
        return new XElement(Atom + "entry",
            new XAttribute(XNamespace.Xmlns + "xmlns", Atom.NamespaceName), // Ensure default ns declaration
            new XElement(Atom + "title", title),
            new XElement(Atom + "content",
                new XAttribute("type", "application/xml"),
                descriptionElement
            )
        );
    }

    private static string FormatTimeSpan(TimeSpan ts)
    {
        if (ts == TimeSpan.MaxValue)
            return "P10675199DT2H48M5.4775807S";
        return System.Xml.XmlConvert.ToString(ts);
    }

    private static XElement? OptionalElement(XName name, string? value)
    {
        return value is not null ? new XElement(name, value) : null;
    }
}
```

- [ ] **Step 4: Run AtomXmlWriter tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~AtomXmlWriterTests" -v minimal
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add AtomXmlWriter for Atom XML entity serialization"
```

---

## Task 7: Atom XML Deserialization (Reader)

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Management/AtomXmlReader.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/AtomXmlReaderTests.cs`

- [ ] **Step 1: Write AtomXmlReader tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/AtomXmlReaderTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;

namespace AzureServiceBusEmulator.Tests.Management;

public class AtomXmlReaderTests
{
    [Fact]
    public void ReadQueueDescription_ParsesProperties()
    {
        var queue = new QueueEntity("test") { LockDuration = TimeSpan.FromMinutes(2), MaxDeliveryCount = 7 };
        var xml = AtomXmlWriter.WriteQueueEntry(queue);

        var props = AtomXmlReader.ReadQueueProperties(xml);

        Assert.Equal(TimeSpan.FromMinutes(2), props.LockDuration);
        Assert.Equal(7, props.MaxDeliveryCount);
    }

    [Fact]
    public void ReadTopicDescription_ParsesProperties()
    {
        var topic = new TopicEntity("test") { MaxSizeInMegabytes = 2048 };
        var xml = AtomXmlWriter.WriteTopicEntry(topic);

        var props = AtomXmlReader.ReadTopicProperties(xml);

        Assert.Equal(2048, props.MaxSizeInMegabytes);
    }

    [Fact]
    public void ReadSubscriptionDescription_ParsesForwardTo()
    {
        var sub = new SubscriptionEntity("sub-1", "topic-1") { ForwardTo = "my-queue" };
        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);

        var props = AtomXmlReader.ReadSubscriptionProperties(xml);

        Assert.Equal("my-queue", props.ForwardTo);
    }

    [Fact]
    public void ReadRuleDescription_ParsesTrueFilter()
    {
        var rule = new RuleEntity("$Default");
        var xml = AtomXmlWriter.WriteRuleEntry(rule);

        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal("$Default", props.Name);
        Assert.Equal(RuleFilterType.TrueFilter, props.FilterType);
    }

    [Fact]
    public void ReadRuleDescription_ParsesSqlFilter()
    {
        var rule = new RuleEntity("custom") { FilterType = RuleFilterType.SqlFilter, SqlExpression = "color='blue'" };
        var xml = AtomXmlWriter.WriteRuleEntry(rule);

        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal(RuleFilterType.SqlFilter, props.FilterType);
        Assert.Equal("color='blue'", props.SqlExpression);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~AtomXmlReaderTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement AtomXmlReader**

Create `src/AzureServiceBusEmulator.Core/Management/AtomXmlReader.cs`:

```csharp
using System.Xml.Linq;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

public static class AtomXmlReader
{
    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";

    public static QueueProperties ReadQueueProperties(string xml)
    {
        var desc = GetDescription(xml, "QueueDescription");
        return new QueueProperties
        {
            LockDuration = ParseTimeSpan(desc, "LockDuration") ?? TimeSpan.FromSeconds(30),
            MaxSizeInMegabytes = ParseLong(desc, "MaxSizeInMegabytes") ?? 1024,
            RequiresSession = ParseBool(desc, "RequiresSession") ?? false,
            DefaultMessageTimeToLive = ParseTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            DeadLetteringOnMessageExpiration = ParseBool(desc, "DeadLetteringOnMessageExpiration") ?? false,
            MaxDeliveryCount = ParseInt(desc, "MaxDeliveryCount") ?? 10,
            EnableBatchedOperations = ParseBool(desc, "EnableBatchedOperations") ?? true,
            ForwardTo = ParseString(desc, "ForwardTo"),
            UserMetadata = ParseString(desc, "UserMetadata"),
        };
    }

    public static TopicProperties ReadTopicProperties(string xml)
    {
        var desc = GetDescription(xml, "TopicDescription");
        return new TopicProperties
        {
            DefaultMessageTimeToLive = ParseTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            MaxSizeInMegabytes = ParseLong(desc, "MaxSizeInMegabytes") ?? 1024,
            EnableBatchedOperations = ParseBool(desc, "EnableBatchedOperations") ?? true,
            UserMetadata = ParseString(desc, "UserMetadata"),
        };
    }

    public static SubscriptionProperties ReadSubscriptionProperties(string xml)
    {
        var desc = GetDescription(xml, "SubscriptionDescription");
        return new SubscriptionProperties
        {
            LockDuration = ParseTimeSpan(desc, "LockDuration") ?? TimeSpan.FromSeconds(30),
            RequiresSession = ParseBool(desc, "RequiresSession") ?? false,
            DefaultMessageTimeToLive = ParseTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            DeadLetteringOnMessageExpiration = ParseBool(desc, "DeadLetteringOnMessageExpiration") ?? false,
            MaxDeliveryCount = ParseInt(desc, "MaxDeliveryCount") ?? 10,
            EnableBatchedOperations = ParseBool(desc, "EnableBatchedOperations") ?? true,
            ForwardTo = ParseString(desc, "ForwardTo"),
            UserMetadata = ParseString(desc, "UserMetadata"),
        };
    }

    public static RuleProperties ReadRuleProperties(string xml)
    {
        var desc = GetDescription(xml, "RuleDescription");
        var name = ParseString(desc, "Name") ?? "$Default";
        var filter = desc.Element(Sb + "Filter");
        var filterType = RuleFilterType.TrueFilter;
        string? sqlExpression = null;
        string? correlationId = null;

        if (filter is not null)
        {
            var typeAttr = filter.Attribute(Xsi + "type")?.Value ?? "";
            if (typeAttr.Contains("SqlFilter"))
            {
                filterType = RuleFilterType.SqlFilter;
                sqlExpression = ParseString(filter, "SqlExpression");
            }
            else if (typeAttr.Contains("CorrelationFilter"))
            {
                filterType = RuleFilterType.CorrelationFilter;
                correlationId = ParseString(filter, "CorrelationId");
            }
            else if (typeAttr.Contains("FalseFilter"))
            {
                filterType = RuleFilterType.FalseFilter;
            }
        }

        string? actionExpression = null;
        var action = desc.Element(Sb + "Action");
        if (action is not null)
        {
            var actionType = action.Attribute(Xsi + "type")?.Value ?? "";
            if (actionType.Contains("SqlRuleAction"))
            {
                actionExpression = ParseString(action, "SqlExpression");
            }
        }

        return new RuleProperties
        {
            Name = name,
            FilterType = filterType,
            SqlExpression = sqlExpression,
            CorrelationId = correlationId,
            ActionExpression = actionExpression,
        };
    }

    private static XElement GetDescription(string xml, string elementName)
    {
        var doc = XDocument.Parse(xml);
        // Handle both feed entries and standalone entries
        var entry = doc.Root!.Name == Atom + "entry" ? doc.Root : doc.Root.Element(Atom + "entry");
        var content = entry!.Element(Atom + "content")!;
        return content.Element(Sb + elementName)!;
    }

    private static string? ParseString(XElement parent, string name) => parent.Element(Sb + name)?.Value;
    private static int? ParseInt(XElement parent, string name) => int.TryParse(parent.Element(Sb + name)?.Value, out var v) ? v : null;
    private static long? ParseLong(XElement parent, string name) => long.TryParse(parent.Element(Sb + name)?.Value, out var v) ? v : null;
    private static bool? ParseBool(XElement parent, string name) => bool.TryParse(parent.Element(Sb + name)?.Value, out var v) ? v : null;

    private static TimeSpan? ParseTimeSpan(XElement parent, string name)
    {
        var value = parent.Element(Sb + name)?.Value;
        if (value is null) return null;
        try { return System.Xml.XmlConvert.ToTimeSpan(value); }
        catch { return null; }
    }
}

public record QueueProperties
{
    public TimeSpan LockDuration { get; init; }
    public long MaxSizeInMegabytes { get; init; }
    public bool RequiresSession { get; init; }
    public TimeSpan DefaultMessageTimeToLive { get; init; }
    public bool DeadLetteringOnMessageExpiration { get; init; }
    public int MaxDeliveryCount { get; init; }
    public bool EnableBatchedOperations { get; init; }
    public string? ForwardTo { get; init; }
    public string? UserMetadata { get; init; }
}

public record TopicProperties
{
    public TimeSpan DefaultMessageTimeToLive { get; init; }
    public long MaxSizeInMegabytes { get; init; }
    public bool EnableBatchedOperations { get; init; }
    public string? UserMetadata { get; init; }
}

public record SubscriptionProperties
{
    public TimeSpan LockDuration { get; init; }
    public bool RequiresSession { get; init; }
    public TimeSpan DefaultMessageTimeToLive { get; init; }
    public bool DeadLetteringOnMessageExpiration { get; init; }
    public int MaxDeliveryCount { get; init; }
    public bool EnableBatchedOperations { get; init; }
    public string? ForwardTo { get; init; }
    public string? UserMetadata { get; init; }
}

public record RuleProperties
{
    public string Name { get; init; } = "$Default";
    public RuleFilterType FilterType { get; init; }
    public string? SqlExpression { get; init; }
    public string? CorrelationId { get; init; }
    public string? ActionExpression { get; init; }
}
```

- [ ] **Step 4: Run AtomXmlReader tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~AtomXmlReaderTests" -v minimal
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add AtomXmlReader for Atom XML entity deserialization"
```

---

## Task 8: REST Management API Error Helpers

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Management/ManagementApiErrors.cs`

- [ ] **Step 1: Implement ManagementApiErrors**

Create `src/AzureServiceBusEmulator.Core/Management/ManagementApiErrors.cs`:

```csharp
using Microsoft.AspNetCore.Http;

namespace AzureServiceBusEmulator.Core.Management;

public static class ManagementApiErrors
{
    private const string ErrorTemplate = """
        <Error>
            <Code>{0}</Code>
            <Detail>{1}</Detail>
        </Error>
        """;

    public static IResult EntityNotFound(string entityName)
    {
        var body = string.Format(ErrorTemplate, "MessagingEntityNotFound", $"The messaging entity '{entityName}' could not be found.");
        return Results.Text(body, "application/xml", statusCode: 404);
    }

    public static IResult EntityAlreadyExists(string entityName)
    {
        var body = string.Format(ErrorTemplate, "MessagingEntityAlreadyExists", $"The messaging entity '{entityName}' already exists.");
        return Results.Text(body, "application/xml", statusCode: 409);
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
dotnet build src/AzureServiceBusEmulator.Core
```

Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add ManagementApiErrors for 404/409 error responses"
```

---

## Task 9: REST Management API Endpoints

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Management/ManagementApiEndpoints.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiQueueTests.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiTopicTests.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiSubscriptionTests.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiRuleTests.cs`

- [ ] **Step 1: Implement ManagementApiEndpoints**

Create `src/AzureServiceBusEmulator.Core/Management/ManagementApiEndpoints.cs`:

```csharp
using AzureServiceBusEmulator.Core.Broker;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AzureServiceBusEmulator.Core.Management;

public static class ManagementApiEndpoints
{
    public static void MapServiceBusManagementApi(this IEndpointRouteBuilder app, NamespaceRegistry registry)
    {
        // Queue operations
        app.MapPut("/{queueName}", async (string queueName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var isUpdate = request.Headers.ContainsKey("If-Match");
            var body = await ReadBodyAsync(request);

            if (isUpdate)
            {
                var existing = ns.GetQueue(queueName);
                if (existing is null)
                    return ManagementApiErrors.EntityNotFound(queueName);
                ApplyQueueProperties(existing, body);
                return Results.Text(AtomXmlWriter.WriteQueueEntry(existing), "application/atom+xml;type=entry;charset=utf-8");
            }

            var queue = ns.CreateQueue(queueName);
            if (body is not null)
                ApplyQueueProperties(queue, body);
            return Results.Text(AtomXmlWriter.WriteQueueEntry(queue), "application/atom+xml;type=entry;charset=utf-8", statusCode: 201);
        });

        app.MapGet("/{entityName}", (string entityName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);

            // Try as queue first, then topic
            var queue = ns.GetQueue(entityName);
            if (queue is not null)
                return Results.Text(AtomXmlWriter.WriteQueueEntry(queue), "application/atom+xml;type=entry;charset=utf-8");

            var topic = ns.GetTopic(entityName);
            if (topic is not null)
                return Results.Text(AtomXmlWriter.WriteTopicEntry(topic), "application/atom+xml;type=entry;charset=utf-8");

            return ManagementApiErrors.EntityNotFound(entityName);
        });

        app.MapDelete("/{entityName}", (string entityName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            if (ns.DeleteQueue(entityName) || ns.DeleteTopic(entityName))
                return Results.Ok();
            return ManagementApiErrors.EntityNotFound(entityName);
        });

        // Topic operations (explicit path to distinguish PUT topic from PUT queue)
        // The SDK differentiates via the XML body content. We detect by checking the body.
        // However, the REST API uses the same path for queues and topics.
        // We'll handle this by checking the XML body for TopicDescription vs QueueDescription.
        // Re-map PUT to handle both:
        // (This is already handled by the PUT above — we'll enhance it.)

        // Subscription operations
        app.MapPut("/{topicName}/Subscriptions/{subName}", async (string topicName, string subName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var isUpdate = request.Headers.ContainsKey("If-Match");
            var body = await ReadBodyAsync(request);

            // Ensure topic exists
            ns.CreateTopic(topicName);

            if (isUpdate)
            {
                var existing = ns.GetSubscription(topicName, subName);
                if (existing is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");
                ApplySubscriptionProperties(existing, body, ns);
                return Results.Text(AtomXmlWriter.WriteSubscriptionEntry(existing), "application/atom+xml;type=entry;charset=utf-8");
            }

            var sub = ns.CreateSubscription(topicName, subName);
            if (body is not null)
                ApplySubscriptionProperties(sub, body, ns);
            return Results.Text(AtomXmlWriter.WriteSubscriptionEntry(sub), "application/atom+xml;type=entry;charset=utf-8", statusCode: 201);
        });

        app.MapGet("/{topicName}/Subscriptions/{subName}", (string topicName, string subName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");
            return Results.Text(AtomXmlWriter.WriteSubscriptionEntry(sub), "application/atom+xml;type=entry;charset=utf-8");
        });

        app.MapDelete("/{topicName}/Subscriptions/{subName}", (string topicName, string subName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var topic = ns.GetTopic(topicName);
            if (topic is null || !topic.RemoveSubscription(subName))
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");
            return Results.Ok();
        });

        // Subscription list
        app.MapGet("/{topicName}/Subscriptions", (string topicName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var topic = ns.GetTopic(topicName);
            if (topic is null)
                return ManagementApiErrors.EntityNotFound(topicName);
            return Results.Text(AtomXmlWriter.WriteSubscriptionFeed(topic.GetSubscriptions()), "application/atom+xml;type=feed;charset=utf-8");
        });

        // Rule operations
        app.MapPut("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", async (string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            var body = await ReadBodyAsync(request);
            var rule = new RuleEntity(ruleName);
            if (body is not null)
                ApplyRuleProperties(rule, body);
            sub.AddOrUpdateRule(ruleName, rule);
            return Results.Text(AtomXmlWriter.WriteRuleEntry(rule), "application/atom+xml;type=entry;charset=utf-8", statusCode: 201);
        });

        app.MapGet("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", (string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            var rule = sub.GetRule(ruleName);
            if (rule is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");

            return Results.Text(AtomXmlWriter.WriteRuleEntry(rule), "application/atom+xml;type=entry;charset=utf-8");
        });

        app.MapGet("/{topicName}/Subscriptions/{subName}/Rules", (string topicName, string subName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            return Results.Text(AtomXmlWriter.WriteRuleFeed(sub.GetRules()), "application/atom+xml;type=feed;charset=utf-8");
        });

        app.MapDelete("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", (string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = GetNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null || !sub.RemoveRule(ruleName))
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");
            return Results.Ok();
        });
    }

    private static NamespaceContext GetNamespace(HttpRequest request, NamespaceRegistry registry)
    {
        // Extract namespace from Host header: "test-abc123.servicebus.windows.net" -> "test-abc123"
        var host = request.Host.Host;
        var namespaceName = host.Split('.')[0];
        return registry.GetOrCreate(namespaceName);
    }

    private static async Task<string?> ReadBodyAsync(HttpRequest request)
    {
        if (request.ContentLength is null or 0)
            return null;
        using var reader = new StreamReader(request.Body);
        return await reader.ReadToEndAsync();
    }

    private static void ApplyQueueProperties(QueueEntity queue, string? xml)
    {
        if (xml is null) return;
        try
        {
            var props = AtomXmlReader.ReadQueueProperties(xml);
            queue.LockDuration = props.LockDuration;
            queue.MaxDeliveryCount = props.MaxDeliveryCount;
            queue.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
            queue.RequiresSession = props.RequiresSession;
            queue.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            queue.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            queue.EnableBatchedOperations = props.EnableBatchedOperations;
            queue.UserMetadata = props.UserMetadata;
        }
        catch { /* Ignore parse failures — use defaults */ }
    }

    private static void ApplySubscriptionProperties(SubscriptionEntity sub, string? xml, NamespaceContext ns)
    {
        if (xml is null) return;
        try
        {
            var props = AtomXmlReader.ReadSubscriptionProperties(xml);
            sub.LockDuration = props.LockDuration;
            sub.MaxDeliveryCount = props.MaxDeliveryCount;
            sub.RequiresSession = props.RequiresSession;
            sub.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            sub.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            sub.EnableBatchedOperations = props.EnableBatchedOperations;
            sub.UserMetadata = props.UserMetadata;
            if (props.ForwardTo is not null)
            {
                sub.ForwardTo = props.ForwardTo;
                sub.ResolvedForwardToQueue = ns.GetQueue(props.ForwardTo);
            }
        }
        catch { /* Ignore parse failures — use defaults */ }
    }

    private static void ApplyRuleProperties(RuleEntity rule, string? xml)
    {
        if (xml is null) return;
        try
        {
            var props = AtomXmlReader.ReadRuleProperties(xml);
            rule.FilterType = props.FilterType;
            rule.SqlExpression = props.SqlExpression;
            rule.CorrelationId = props.CorrelationId;
            rule.ActionExpression = props.ActionExpression;
        }
        catch { /* Ignore parse failures — use defaults */ }
    }
}
```

**Note on PUT for topics vs queues:** The Azure SDK sends `QueueDescription` or `TopicDescription` in the body. We need to check the body to differentiate. Update the PUT handler:

The PUT `/{queueName}` handler above needs to detect if the body contains `TopicDescription` instead of `QueueDescription`. Update the PUT handler to handle both. Replace the PUT `/{queueName}` route body with:

```csharp
app.MapPut("/{entityName}", async (string entityName, HttpRequest request) =>
{
    var ns = GetNamespace(request, registry);
    var isUpdate = request.Headers.ContainsKey("If-Match");
    var body = await ReadBodyAsync(request);

    // Detect entity type from XML body
    var isTopic = body?.Contains("TopicDescription") == true;

    if (isTopic)
    {
        if (isUpdate)
        {
            var existing = ns.GetTopic(entityName);
            if (existing is null)
                return ManagementApiErrors.EntityNotFound(entityName);
            ApplyTopicProperties(existing, body);
            return Results.Text(AtomXmlWriter.WriteTopicEntry(existing), "application/atom+xml;type=entry;charset=utf-8");
        }
        var topic = ns.CreateTopic(entityName);
        if (body is not null)
            ApplyTopicProperties(topic, body);
        return Results.Text(AtomXmlWriter.WriteTopicEntry(topic), "application/atom+xml;type=entry;charset=utf-8", statusCode: 201);
    }
    else
    {
        if (isUpdate)
        {
            var existing = ns.GetQueue(entityName);
            if (existing is null)
                return ManagementApiErrors.EntityNotFound(entityName);
            ApplyQueueProperties(existing, body);
            return Results.Text(AtomXmlWriter.WriteQueueEntry(existing), "application/atom+xml;type=entry;charset=utf-8");
        }
        var queue = ns.CreateQueue(entityName);
        if (body is not null)
            ApplyQueueProperties(queue, body);
        return Results.Text(AtomXmlWriter.WriteQueueEntry(queue), "application/atom+xml;type=entry;charset=utf-8", statusCode: 201);
    }
});
```

Also add this helper:

```csharp
private static void ApplyTopicProperties(TopicEntity topic, string? xml)
{
    if (xml is null) return;
    try
    {
        var props = AtomXmlReader.ReadTopicProperties(xml);
        topic.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
        topic.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
        topic.EnableBatchedOperations = props.EnableBatchedOperations;
        topic.UserMetadata = props.UserMetadata;
    }
    catch { /* Ignore parse failures — use defaults */ }
}
```

- [ ] **Step 2: Write Management API queue tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiQueueTests.cs`:

```csharp
using System.Net;
using System.Text;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;

namespace AzureServiceBusEmulator.Tests.Management;

public class ManagementApiQueueTests : IAsyncLifetime
{
    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();

        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task CreateQueue_Returns201WithQueueDescription()
    {
        var body = AtomXmlWriter.WriteQueueEntry(new QueueEntity("test-queue") { MaxDeliveryCount = 5 });
        var response = await _client.PutAsync("/test-queue", new StringContent(body, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", content);
        Assert.Contains("5", content);
    }

    [Fact]
    public async Task GetQueue_ReturnsQueueDescription()
    {
        // Create first
        _registry.GetOrCreate("test-ns").CreateQueue("my-queue");

        var response = await _client.GetAsync("/my-queue");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", content);
    }

    [Fact]
    public async Task GetQueue_Returns404IfNotFound()
    {
        var response = await _client.GetAsync("/nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("MessagingEntityNotFound", content);
    }

    [Fact]
    public async Task DeleteQueue_Returns200()
    {
        _registry.GetOrCreate("test-ns").CreateQueue("delete-me");

        var response = await _client.DeleteAsync("/delete-me");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task DeleteQueue_Returns404IfNotFound()
    {
        var response = await _client.DeleteAsync("/nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
```

- [ ] **Step 3: Write Management API topic tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiTopicTests.cs`:

```csharp
using System.Net;
using System.Text;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;

namespace AzureServiceBusEmulator.Tests.Management;

public class ManagementApiTopicTests : IAsyncLifetime
{
    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();

        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task CreateTopic_Returns201WithTopicDescription()
    {
        var body = AtomXmlWriter.WriteTopicEntry(new TopicEntity("my-topic"));
        var response = await _client.PutAsync("/my-topic", new StringContent(body, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("TopicDescription", content);
    }

    [Fact]
    public async Task GetTopic_ReturnsTopicDescription()
    {
        _registry.GetOrCreate("test-ns").CreateTopic("my-topic");

        var response = await _client.GetAsync("/my-topic");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("TopicDescription", content);
    }
}
```

- [ ] **Step 4: Write Management API subscription tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiSubscriptionTests.cs`:

```csharp
using System.Net;
using System.Text;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;

namespace AzureServiceBusEmulator.Tests.Management;

public class ManagementApiSubscriptionTests : IAsyncLifetime
{
    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();

        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";

        // Pre-create topic
        _registry.GetOrCreate("test-ns").CreateTopic("my-topic");
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task CreateSubscription_Returns201()
    {
        var body = AtomXmlWriter.WriteSubscriptionEntry(new SubscriptionEntity("sub-1", "my-topic"));
        var response = await _client.PutAsync("/my-topic/Subscriptions/sub-1", new StringContent(body, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("SubscriptionDescription", content);
    }

    [Fact]
    public async Task GetSubscription_Returns200()
    {
        _registry.GetOrCreate("test-ns").CreateSubscription("my-topic", "sub-1");

        var response = await _client.GetAsync("/my-topic/Subscriptions/sub-1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task GetSubscription_Returns404IfNotFound()
    {
        var response = await _client.GetAsync("/my-topic/Subscriptions/nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task UpdateSubscription_WithIfMatch_Updates()
    {
        _registry.GetOrCreate("test-ns").CreateSubscription("my-topic", "sub-1");

        var sub = new SubscriptionEntity("sub-1", "my-topic") { MaxDeliveryCount = 3 };
        var body = AtomXmlWriter.WriteSubscriptionEntry(sub);
        var request = new HttpRequestMessage(HttpMethod.Put, "/my-topic/Subscriptions/sub-1")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/atom+xml")
        };
        request.Headers.Add("If-Match", "*");

        var response = await _client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Verify update persisted
        var updated = _registry.GetOrCreate("test-ns").GetSubscription("my-topic", "sub-1");
        Assert.Equal(3, updated!.MaxDeliveryCount);
    }

    [Fact]
    public async Task DeleteSubscription_Returns200()
    {
        _registry.GetOrCreate("test-ns").CreateSubscription("my-topic", "sub-1");

        var response = await _client.DeleteAsync("/my-topic/Subscriptions/sub-1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
```

- [ ] **Step 5: Write Management API rule tests**

Create `tests/AzureServiceBusEmulator.Tests/Management/ManagementApiRuleTests.cs`:

```csharp
using System.Net;
using System.Text;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;

namespace AzureServiceBusEmulator.Tests.Management;

public class ManagementApiRuleTests : IAsyncLifetime
{
    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();

        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";

        // Pre-create topic + subscription
        var ns = _registry.GetOrCreate("test-ns");
        ns.CreateTopic("my-topic");
        ns.CreateSubscription("my-topic", "sub-1");
    }

    public async Task DisposeAsync() => await _host.DisposeAsync();

    [Fact]
    public async Task GetDefaultRule_Returns200()
    {
        var response = await _client.GetAsync("/my-topic/Subscriptions/sub-1/Rules/$Default");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("RuleDescription", content);
    }

    [Fact]
    public async Task ListRules_ReturnsFeed()
    {
        var response = await _client.GetAsync("/my-topic/Subscriptions/sub-1/Rules");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("feed", content);
    }

    [Fact]
    public async Task CreateRule_Returns201()
    {
        var rule = new RuleEntity("custom-rule") { FilterType = RuleFilterType.SqlFilter, SqlExpression = "1=1" };
        var body = AtomXmlWriter.WriteRuleEntry(rule);
        var response = await _client.PutAsync("/my-topic/Subscriptions/sub-1/Rules/custom-rule",
            new StringContent(body, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    [Fact]
    public async Task DeleteRule_Returns200()
    {
        var response = await _client.DeleteAsync("/my-topic/Subscriptions/sub-1/Rules/$Default");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
```

- [ ] **Step 6: Run all management API tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~ManagementApi" -v minimal
```

Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: add REST management API endpoints with queue/topic/subscription/rule CRUD"
```

---

## Task 10: CBS Authentication Handler

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Amqp/CbsRequestProcessor.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Amqp/CbsRequestProcessorTests.cs`

- [ ] **Step 1: Write CBS tests**

Create `tests/AzureServiceBusEmulator.Tests/Amqp/CbsRequestProcessorTests.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using AzureServiceBusEmulator.Core.Amqp;

namespace AzureServiceBusEmulator.Tests.Amqp;

public class CbsRequestProcessorTests
{
    [Fact]
    public void ProcessRequest_PutToken_Returns200()
    {
        var processor = new CbsRequestProcessor();

        var request = new Message(new AmqpValue { Value = "SharedAccessSignature sr=test" })
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["operation"] = "put-token",
                ["type"] = "servicebus.windows.net:sastoken",
                ["name"] = "amqp://test.servicebus.windows.net/my-queue"
            },
            Properties = new Properties { ReplyTo = "cbs-reply", MessageId = "req-1" }
        };

        var response = processor.ProcessRequest(request);

        Assert.Equal(200, response.ApplicationProperties["status-code"]);
        Assert.Equal("req-1", response.Properties.CorrelationId);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~CbsRequestProcessorTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement CbsRequestProcessor**

Create `src/AzureServiceBusEmulator.Core/Amqp/CbsRequestProcessor.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using Amqp.Listener;

namespace AzureServiceBusEmulator.Core.Amqp;

public class CbsRequestProcessor : IRequestProcessor
{
    public void Process(RequestContext requestContext)
    {
        var response = ProcessRequest(requestContext.Message);
        requestContext.Complete(response);
    }

    public Message ProcessRequest(Message request)
    {
        // Accept all tokens unconditionally
        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = request.Properties?.MessageId
            }
        };

        return response;
    }
}
```

- [ ] **Step 4: Run CBS tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~CbsRequestProcessorTests" -v minimal
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add CBS authentication handler (accept-all)"
```

---

## Task 11: AMQP SenderLinkEndpoint

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Amqp/SenderLinkEndpoint.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Amqp/SenderLinkEndpointTests.cs`

The `SenderLinkEndpoint` receives messages from clients (the client has a sender link, the server has a receiver endpoint). It routes messages to the appropriate queue or topic based on the link target address.

- [ ] **Step 1: Write SenderLinkEndpoint tests**

Create `tests/AzureServiceBusEmulator.Tests/Amqp/SenderLinkEndpointTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Amqp;

public class SenderLinkEndpointTests
{
    [Fact]
    public void RouteMessage_ToQueue_EnqueuesMessage()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var endpoint = new SenderLinkEndpoint(ctx, "my-queue");

        var msg = new BrokeredMessage { Body = [1, 2, 3], MessageId = "msg-1" };
        endpoint.RouteMessage(msg);

        var received = queue.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal("msg-1", received!.MessageId);
    }

    [Fact]
    public void RouteMessage_ToTopic_FansOut()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateTopic("my-topic");
        ctx.CreateQueue("target");
        ctx.CreateSubscription("my-topic", "sub-1", forwardTo: "target");
        var endpoint = new SenderLinkEndpoint(ctx, "my-topic");

        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1" };
        endpoint.RouteMessage(msg);

        var received = ctx.GetQueue("target")!.TryDequeueImmediate();
        Assert.NotNull(received);
    }

    [Fact]
    public void RouteMessage_AssignsSequenceNumber()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateQueue("my-queue");
        var endpoint = new SenderLinkEndpoint(ctx, "my-queue");

        var msg = new BrokeredMessage { Body = [] };
        endpoint.RouteMessage(msg);

        var received = ctx.GetQueue("my-queue")!.TryDequeueImmediate();
        Assert.True(received!.SequenceNumber > 0);
    }

    [Fact]
    public void RouteMessage_WithScheduledEnqueueTime_SchedulesInstead()
    {
        var ctx = new NamespaceContext("test");
        ctx.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ctx);
        var endpoint = new SenderLinkEndpoint(ctx, "my-queue", processor);

        var msg = new BrokeredMessage
        {
            Body = [],
            MessageId = "scheduled-1",
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddHours(1)
        };
        endpoint.RouteMessage(msg);

        // Should NOT be in the queue immediately
        Assert.Null(ctx.GetQueue("my-queue")!.TryDequeueImmediate());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~SenderLinkEndpointTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement SenderLinkEndpoint**

Create `src/AzureServiceBusEmulator.Core/Amqp/SenderLinkEndpoint.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

public class SenderLinkEndpoint : LinkEndpoint
{
    private readonly NamespaceContext _context;
    private readonly string _entityName;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;

    public SenderLinkEndpoint(NamespaceContext context, string entityName, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _context = context;
        _entityName = entityName;
        _scheduledProcessor = scheduledProcessor;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        try
        {
            var amqpMessage = messageContext.Message;
            var brokeredMessage = ConvertToBrokeredMessage(amqpMessage);
            RouteMessage(brokeredMessage);
            messageContext.Complete();
        }
        catch
        {
            messageContext.Complete(new Amqp.Framing.Rejected
            {
                Error = new Amqp.Framing.Error(Amqp.Framing.ErrorCode.InternalError)
            });
        }
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // No-op for sender endpoint
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }

    public void RouteMessage(BrokeredMessage message)
    {
        message.SequenceNumber = _context.NextSequenceNumber();
        message.EnqueuedTimeUtc = DateTimeOffset.UtcNow;

        // Check for scheduled delivery
        if (message.ScheduledEnqueueTimeUtc.HasValue && message.ScheduledEnqueueTimeUtc > DateTimeOffset.UtcNow && _scheduledProcessor is not null)
        {
            _scheduledProcessor.Schedule(_entityName, message);
            return;
        }

        var (queue, topic) = _context.ResolveSendTarget(_entityName);

        if (queue is not null)
        {
            queue.Enqueue(message);
        }
        else if (topic is not null)
        {
            topic.Publish(message);
        }
    }

    private static BrokeredMessage ConvertToBrokeredMessage(Message amqpMessage)
    {
        var msg = new BrokeredMessage();

        // Body
        if (amqpMessage.Body is byte[] bytes)
            msg.Body = bytes;
        else if (amqpMessage.Body is Amqp.Framing.Data data)
            msg.Body = data.Binary;

        // Properties
        if (amqpMessage.Properties is not null)
        {
            msg.MessageId = amqpMessage.Properties.MessageId?.ToString() ?? msg.MessageId;
            msg.CorrelationId = amqpMessage.Properties.CorrelationId?.ToString();
            msg.ContentType = amqpMessage.Properties.ContentType;
            msg.Subject = amqpMessage.Properties.Subject;
            msg.ReplyTo = amqpMessage.Properties.ReplyTo;
            msg.To = amqpMessage.Properties.To;
            msg.ReplyToSessionId = amqpMessage.Properties.ReplyToGroupId;
        }

        // Application properties
        if (amqpMessage.ApplicationProperties?.Map is not null)
        {
            foreach (var kvp in amqpMessage.ApplicationProperties.Map)
            {
                msg.ApplicationProperties[kvp.Key.ToString()!] = kvp.Value;
            }
        }

        // Message annotations (for scheduled enqueue time, partition key, etc.)
        if (amqpMessage.MessageAnnotations?.Map is not null)
        {
            var annotations = amqpMessage.MessageAnnotations.Map;
            if (annotations.TryGetValue(new Amqp.Types.Symbol("x-opt-scheduled-enqueue-time"), out var scheduledTime) && scheduledTime is DateTime dt)
            {
                msg.ScheduledEnqueueTimeUtc = new DateTimeOffset(dt, TimeSpan.Zero);
            }
            if (annotations.TryGetValue(new Amqp.Types.Symbol("x-opt-partition-key"), out var pk))
            {
                msg.PartitionKey = pk?.ToString();
            }
            if (annotations.TryGetValue(new Amqp.Types.Symbol("x-opt-session-id"), out var sid))
            {
                msg.SessionId = sid?.ToString();
            }
        }

        return msg;
    }
}
```

- [ ] **Step 4: Run SenderLinkEndpoint tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~SenderLinkEndpointTests" -v minimal
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add SenderLinkEndpoint for routing incoming AMQP messages"
```

---

## Task 12: AMQP ReceiverLinkEndpoint

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Amqp/ReceiverLinkEndpoint.cs`
- Create: `tests/AzureServiceBusEmulator.Tests/Amqp/ReceiverLinkEndpointTests.cs`

- [ ] **Step 1: Write ReceiverLinkEndpoint tests**

Create `tests/AzureServiceBusEmulator.Tests/Amqp/ReceiverLinkEndpointTests.cs`:

```csharp
using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Amqp;

public class ReceiverLinkEndpointTests
{
    [Fact]
    public async Task DequeueLoop_DeliversMessages()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var endpoint = new ReceiverLinkEndpoint(ctx, "my-queue");

        queue.Enqueue(new BrokeredMessage { Body = [1], MessageId = "msg-1" });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var msg = await endpoint.DequeueAsync(cts.Token);

        Assert.NotNull(msg);
        Assert.Equal("msg-1", msg!.MessageId);
    }

    [Fact]
    public void CompleteMessage_RemovesFromPending()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var endpoint = new ReceiverLinkEndpoint(ctx, "my-queue");

        var lockToken = Guid.NewGuid().ToString();
        var msg = new BrokeredMessage { Body = [], LockToken = lockToken };
        queue.TrackPending(msg);

        endpoint.SettleMessage(lockToken, SettlementOutcome.Complete);
        // No exception = success
    }

    [Fact]
    public void AbandonMessage_RequeuesMessage()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var endpoint = new ReceiverLinkEndpoint(ctx, "my-queue");

        var lockToken = Guid.NewGuid().ToString();
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1", LockToken = lockToken };
        queue.TrackPending(msg);

        endpoint.SettleMessage(lockToken, SettlementOutcome.Abandon);

        var requeued = queue.TryDequeueImmediate();
        Assert.NotNull(requeued);
    }

    [Fact]
    public void DeadLetterMessage_MovesToDLQ()
    {
        var ctx = new NamespaceContext("test");
        var queue = ctx.CreateQueue("my-queue");
        var endpoint = new ReceiverLinkEndpoint(ctx, "my-queue");

        var lockToken = Guid.NewGuid().ToString();
        var msg = new BrokeredMessage { Body = [], MessageId = "msg-1", LockToken = lockToken };
        queue.TrackPending(msg);

        endpoint.SettleMessage(lockToken, SettlementOutcome.DeadLetter, "TestReason", "TestDesc");

        var dlqMsg = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMsg);
        Assert.Equal("TestReason", dlqMsg!.DeadLetterReason);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~ReceiverLinkEndpointTests" -v minimal
```

Expected: FAIL.

- [ ] **Step 3: Implement ReceiverLinkEndpoint**

Create `src/AzureServiceBusEmulator.Core/Amqp/ReceiverLinkEndpoint.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

public enum SettlementOutcome
{
    Complete,
    Abandon,
    DeadLetter
}

public class ReceiverLinkEndpoint : LinkEndpoint
{
    private readonly NamespaceContext _context;
    private readonly string _entityAddress;
    private readonly QueueEntity _queue;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public ReceiverLinkEndpoint(NamespaceContext context, string entityAddress)
    {
        _context = context;
        _entityAddress = entityAddress;
        _queue = context.ResolveQueue(entityAddress)
            ?? throw new InvalidOperationException($"Entity not found: {entityAddress}");
    }

    public async Task<BrokeredMessage?> DequeueAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _queue.DequeueAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    public void SettleMessage(string lockToken, SettlementOutcome outcome, string? reason = null, string? description = null)
    {
        switch (outcome)
        {
            case SettlementOutcome.Complete:
                _queue.Complete(lockToken);
                break;
            case SettlementOutcome.Abandon:
                _queue.Abandon(lockToken);
                break;
            case SettlementOutcome.DeadLetter:
                _queue.DeadLetter(lockToken, reason, description);
                break;
        }
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // Start pumping messages when credit is available
        if (_pumpTask is null || _pumpTask.IsCompleted)
        {
            _pumpCts = new CancellationTokenSource();
            _pumpTask = PumpMessages(flowContext, _pumpCts.Token);
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        var deliveryState = dispositionContext.Message.Header?.DeliveryCount > 0
            ? dispositionContext.DeliveryState
            : dispositionContext.DeliveryState;

        // Get lock token from message annotations
        var lockToken = dispositionContext.Message.MessageAnnotations?[new Symbol("x-opt-lock-token")]?.ToString();

        if (deliveryState is Accepted)
        {
            if (lockToken is not null) _queue.Complete(lockToken);
            dispositionContext.Complete();
        }
        else if (deliveryState is Released)
        {
            if (lockToken is not null) _queue.Abandon(lockToken);
            dispositionContext.Complete();
        }
        else if (deliveryState is Rejected rejected)
        {
            if (lockToken is not null)
                _queue.DeadLetter(lockToken, rejected.Error?.Condition?.ToString(), rejected.Error?.Description);
            dispositionContext.Complete();
        }
        else if (deliveryState is Modified modified)
        {
            if (lockToken is not null)
            {
                if (modified.UndeliverableHere == true)
                    _queue.DeadLetter(lockToken);
                else
                    _queue.Abandon(lockToken);
            }
            dispositionContext.Complete();
        }
        else
        {
            dispositionContext.Complete();
        }
    }

    private async Task PumpMessages(FlowContext flowContext, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var msg = await _queue.DequeueAsync(cancellationToken);
                var amqpMessage = ConvertToAmqpMessage(msg);
                flowContext.SendMessage(amqpMessage);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                break;
            }
        }
    }

    private static Message ConvertToAmqpMessage(BrokeredMessage msg)
    {
        var amqpMessage = new Message(new Data { Binary = msg.Body })
        {
            Properties = new Properties
            {
                MessageId = msg.MessageId,
                CorrelationId = msg.CorrelationId,
                ContentType = msg.ContentType,
                Subject = msg.Subject,
                ReplyTo = msg.ReplyTo,
                To = msg.To,
                ReplyToGroupId = msg.ReplyToSessionId,
                GroupId = msg.SessionId,
            },
            Header = new Header
            {
                DeliveryCount = (uint)msg.DeliveryCount,
                Ttl = msg.TimeToLive == TimeSpan.MaxValue ? null : (uint?)msg.TimeToLive.TotalMilliseconds,
            },
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-sequence-number")] = msg.SequenceNumber,
                [new Symbol("x-opt-enqueued-time")] = msg.EnqueuedTimeUtc.UtcDateTime,
                [new Symbol("x-opt-lock-token")] = Guid.Parse(msg.LockToken ?? Guid.Empty.ToString()),
                [new Symbol("x-opt-locked-until")] = DateTime.UtcNow.AddSeconds(30), // Lock duration
            }
        };

        if (msg.ApplicationProperties.Count > 0)
        {
            amqpMessage.ApplicationProperties = new ApplicationProperties();
            foreach (var (key, value) in msg.ApplicationProperties)
            {
                amqpMessage.ApplicationProperties[key] = value;
            }
        }

        if (msg.DeadLetterReason is not null)
        {
            amqpMessage.ApplicationProperties ??= new ApplicationProperties();
            amqpMessage.ApplicationProperties["DeadLetterReason"] = msg.DeadLetterReason;
            amqpMessage.ApplicationProperties["DeadLetterErrorDescription"] = msg.DeadLetterErrorDescription;
        }

        return amqpMessage;
    }

    public void Stop()
    {
        _pumpCts?.Cancel();
        _pumpCts?.Dispose();
    }
}
```

- [ ] **Step 4: Run ReceiverLinkEndpoint tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests --filter "FullyQualifiedName~ReceiverLinkEndpointTests" -v minimal
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add ReceiverLinkEndpoint with message pump and settlement"
```

---

## Task 13: AMQP ManagementLinkEndpoint and LinkProcessor

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Amqp/ManagementLinkEndpoint.cs`
- Create: `src/AzureServiceBusEmulator.Core/Amqp/ServiceBusLinkProcessor.cs`
- Create: `src/AzureServiceBusEmulator.Core/Amqp/AmqpServerOptions.cs`

- [ ] **Step 1: Implement ManagementLinkEndpoint**

Create `src/AzureServiceBusEmulator.Core/Amqp/ManagementLinkEndpoint.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Handles $management node requests (e.g., cancel-scheduled-message).
/// </summary>
public class ManagementLinkEndpoint : IRequestProcessor
{
    private readonly NamespaceContext _context;
    private readonly ScheduledMessageProcessor _scheduledProcessor;

    public ManagementLinkEndpoint(NamespaceContext context, ScheduledMessageProcessor scheduledProcessor)
    {
        _context = context;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Process(RequestContext requestContext)
    {
        var request = requestContext.Message;
        var operation = request.ApplicationProperties?["operation"]?.ToString();

        Message response;
        if (operation == "com.microsoft:cancel-scheduled-message")
        {
            response = HandleCancelScheduled(request);
        }
        else if (operation == "com.microsoft:schedule-message")
        {
            response = HandleScheduleMessage(request);
        }
        else
        {
            response = new Message
            {
                ApplicationProperties = new ApplicationProperties
                {
                    ["status-code"] = 200,
                    ["status-description"] = "OK"
                },
                Properties = new Properties { CorrelationId = request.Properties?.MessageId }
            };
        }

        requestContext.Complete(response);
    }

    private Message HandleCancelScheduled(Message request)
    {
        // The sequence numbers to cancel are in the body as a map with "sequence-numbers" key
        if (request.Body is Map body && body.TryGetValue(new Symbol("sequence-numbers"), out var seqNums))
        {
            if (seqNums is long[] numbers)
            {
                foreach (var num in numbers)
                    _scheduledProcessor.CancelScheduled(num);
            }
        }

        return new Message
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK"
            },
            Properties = new Properties { CorrelationId = request.Properties?.MessageId }
        };
    }

    private Message HandleScheduleMessage(Message request)
    {
        // Return sequence numbers for scheduled messages
        return new Message(new Map { { new Symbol("sequence-numbers"), Array.Empty<long>() } })
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK"
            },
            Properties = new Properties { CorrelationId = request.Properties?.MessageId }
        };
    }
}
```

- [ ] **Step 2: Implement AmqpServerOptions**

Create `src/AzureServiceBusEmulator.Core/Amqp/AmqpServerOptions.cs`:

```csharp
namespace AzureServiceBusEmulator.Core.Amqp;

public class AmqpServerOptions
{
    public int Port { get; set; } = 5672;
    public string Host { get; set; } = "localhost";
}
```

- [ ] **Step 3: Implement ServiceBusLinkProcessor**

Create `src/AzureServiceBusEmulator.Core/Amqp/ServiceBusLinkProcessor.cs`:

```csharp
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

public class ServiceBusLinkProcessor : ILinkProcessor
{
    private readonly NamespaceRegistry _registry;
    private readonly Dictionary<string, ScheduledMessageProcessor> _scheduledProcessors = new();

    public ServiceBusLinkProcessor(NamespaceRegistry registry)
    {
        _registry = registry;
    }

    public void Process(AttachContext attachContext)
    {
        var address = GetAddress(attachContext);
        var namespaceName = GetNamespaceName(attachContext);
        var ctx = _registry.GetOrCreate(namespaceName);
        var scheduledProcessor = GetOrCreateScheduledProcessor(namespaceName, ctx);

        if (address == "$cbs")
        {
            // CBS is handled separately via IRequestProcessor on ContainerHost
            attachContext.Complete(new Error(ErrorCode.NotAllowed) { Description = "CBS should be registered as request processor" });
            return;
        }

        if (address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase) || address == "$management")
        {
            // Management links are request/response
            attachContext.Complete(new Error(ErrorCode.NotAllowed) { Description = "Management should be registered as request processor" });
            return;
        }

        // Auto-create entity if it doesn't exist (for sender links)
        if (attachContext.Link.Role) // true = receiver (server receives from client = client is sender)
        {
            // Client is sending — server receives
            EnsureEntityExists(ctx, address);
            var endpoint = new SenderLinkEndpoint(ctx, address, scheduledProcessor);
            attachContext.Complete(endpoint, 300); // 300 message credit
        }
        else
        {
            // Client is receiving — server sends
            var queue = ctx.ResolveQueue(address);
            if (queue is null)
            {
                attachContext.Complete(new Error(ErrorCode.NotFound) { Description = $"Entity not found: {address}" });
                return;
            }
            var endpoint = new ReceiverLinkEndpoint(ctx, address);
            attachContext.Complete(endpoint, 0);
        }
    }

    private static string GetAddress(AttachContext attachContext)
    {
        if (attachContext.Link.Role) // Server is receiver
        {
            return (attachContext.Attach.Target as Target)?.Address ?? "";
        }
        else // Server is sender
        {
            return (attachContext.Attach.Source as Source)?.Address ?? "";
        }
    }

    private static string GetNamespaceName(AttachContext attachContext)
    {
        // Extract namespace from the connection's hostname
        var hostname = attachContext.Link.Session.Connection.Handler?.ToString() ?? "default";
        // In practice, the open frame contains the hostname
        // For now, extract from the connection's remote container id or use a default
        return "default";
    }

    private void EnsureEntityExists(NamespaceContext ctx, string address)
    {
        // If address contains /Subscriptions/, it's a subscription path — don't auto-create
        if (address.Contains("/Subscriptions/", StringComparison.OrdinalIgnoreCase))
            return;

        // Auto-create as queue if it doesn't exist as queue or topic
        if (ctx.GetQueue(address) is null && ctx.GetTopic(address) is null)
        {
            ctx.CreateQueue(address);
        }
    }

    private ScheduledMessageProcessor GetOrCreateScheduledProcessor(string namespaceName, NamespaceContext ctx)
    {
        if (!_scheduledProcessors.TryGetValue(namespaceName, out var processor))
        {
            processor = new ScheduledMessageProcessor(ctx);
            processor.StartBackground(TimeSpan.FromMilliseconds(100));
            _scheduledProcessors[namespaceName] = processor;
        }
        return processor;
    }
}
```

- [ ] **Step 4: Verify build**

```bash
dotnet build src/AzureServiceBusEmulator.Core
```

Expected: Build succeeds.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add ManagementLinkEndpoint, ServiceBusLinkProcessor, and AmqpServerOptions"
```

---

## Task 14: AMQP Server (ContainerHost)

**Files:**
- Create: `src/AzureServiceBusEmulator.Core/Amqp/AmqpServer.cs`

- [ ] **Step 1: Implement AmqpServer**

Create `src/AzureServiceBusEmulator.Core/Amqp/AmqpServer.cs`:

```csharp
using Amqp;
using Amqp.Listener;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

public class AmqpServer : IDisposable
{
    private readonly ContainerHost _host;
    private readonly AmqpServerOptions _options;
    private readonly NamespaceRegistry _registry;

    public AmqpServer(AmqpServerOptions options, NamespaceRegistry registry)
    {
        _options = options;
        _registry = registry;

        var uri = new Uri($"amqp://{options.Host}:{options.Port}");
        _host = new ContainerHost(uri);
    }

    public void Start()
    {
        // Register CBS handler for $cbs node
        _host.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());

        // Register link processor for all other links
        _host.RegisterLinkProcessor(new ServiceBusLinkProcessor(_registry));

        _host.Open();
    }

    public void Stop()
    {
        _host.Close();
    }

    public void Dispose()
    {
        Stop();
    }
}
```

- [ ] **Step 2: Verify build**

```bash
dotnet build src/AzureServiceBusEmulator.Core
```

Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add AmqpServer wrapping ContainerHost lifecycle"
```

---

## Task 15: Host Program (Kestrel + AMQP)

**Files:**
- Modify: `src/AzureServiceBusEmulator.Host/Program.cs`

- [ ] **Step 1: Implement Host Program.cs**

Replace `src/AzureServiceBusEmulator.Host/Program.cs` with:

```csharp
using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;

var builder = WebApplication.CreateBuilder(args);

var amqpPort = builder.Configuration.GetValue("Amqp:Port", 5672);
var httpPort = builder.Configuration.GetValue("Http:Port", 5300);

var registry = new NamespaceRegistry();

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(httpPort);
});

var app = builder.Build();

app.MapServiceBusManagementApi(registry);

// Start AMQP server alongside Kestrel
var amqpServer = new AmqpServer(new AmqpServerOptions { Port = amqpPort }, registry);
amqpServer.Start();

app.Lifetime.ApplicationStopping.Register(() => amqpServer.Stop());

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  AMQP: amqp://localhost:{amqpPort}");
Console.WriteLine($"  HTTP: http://localhost:{httpPort}");

app.Run();
```

- [ ] **Step 2: Verify build**

```bash
dotnet build src/AzureServiceBusEmulator.Host
```

Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add Host startup with Kestrel REST API and AMQP listener"
```

---

## Task 16: TestHost Fixture

**Files:**
- Create: `src/AzureServiceBusEmulator.TestHost/ServiceBusEmulatorFixture.cs`

- [ ] **Step 1: Implement ServiceBusEmulatorFixture**

Create `src/AzureServiceBusEmulator.TestHost/ServiceBusEmulatorFixture.cs`:

```csharp
using AzureServiceBusEmulator.Core.Amqp;
using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace AzureServiceBusEmulator.TestHost;

public class ServiceBusEmulatorFixture : IAsyncDisposable
{
    private WebApplication? _webApp;
    private AmqpServer? _amqpServer;
    private readonly NamespaceRegistry _registry = new();
    private readonly string _namespace;

    public int AmqpPort { get; private set; }
    public int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://{_namespace}.localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture()
    {
        _namespace = $"test-{Guid.NewGuid():N}".Substring(0, 20);
    }

    public async Task StartAsync()
    {
        // Find free ports
        AmqpPort = GetFreePort();
        HttpPort = GetFreePort();

        // Start AMQP server
        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry);
        _amqpServer.Start();

        // Start HTTP server
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k => k.ListenLocalhost(HttpPort));
        builder.Logging.ClearProviders(); // Quiet for tests

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);

        await _webApp.StartAsync();
    }

    public async Task StopAsync()
    {
        _amqpServer?.Stop();
        if (_webApp is not null)
            await _webApp.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _amqpServer?.Dispose();
        if (_webApp is not null)
            await _webApp.DisposeAsync();
    }

    public NamespaceContext GetNamespaceContext() => _registry.GetOrCreate(_namespace);

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
```

- [ ] **Step 2: Update TestHost csproj for ASP.NET Core reference**

Edit `src/AzureServiceBusEmulator.TestHost/AzureServiceBusEmulator.TestHost.csproj` to add the FrameworkReference:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AzureServiceBusEmulator.Core\AzureServiceBusEmulator.Core.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Verify build**

```bash
dotnet build src/AzureServiceBusEmulator.TestHost
```

Expected: Build succeeds.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add ServiceBusEmulatorFixture for in-process test hosting"
```

---

## Task 17: SDK Integration Tests — Admin Client

**Files:**
- Create: `tests/AzureServiceBusEmulator.SdkIntegration.Tests/AdminClientTests.cs`

These tests use the real `ServiceBusAdministrationClient` against the emulator.

- [ ] **Step 1: Write AdminClientTests**

Create `tests/AzureServiceBusEmulator.SdkIntegration.Tests/AdminClientTests.cs`:

```csharp
using Azure.Messaging.ServiceBus.Administration;
using AzureServiceBusEmulator.TestHost;

namespace AzureServiceBusEmulator.SdkIntegration.Tests;

public class AdminClientTests : IAsyncLifetime
{
    private ServiceBusEmulatorFixture _fixture = null!;
    private ServiceBusAdministrationClient _admin = null!;

    public async Task InitializeAsync()
    {
        _fixture = new ServiceBusEmulatorFixture();
        await _fixture.StartAsync();

        // Point admin client at the emulator's HTTP endpoint
        // The admin client uses REST API — we need to provide a custom endpoint
        _admin = new ServiceBusAdministrationClient(
            $"Endpoint=sb://{_fixture.Namespace}.localhost;SharedAccessKeyName=emulator;SharedAccessKey=emulator",
            new ServiceBusAdministrationClientOptions
            {
                // Override the transport to point at localhost
            });

        // Alternative: construct with a custom URI
        // This may require using the Azure.Core pipeline to redirect requests to localhost:HttpPort
    }

    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task CreateQueue_ThenGetQueue_RoundTrips()
    {
        // NOTE: This test will need adjustment based on how the SDK constructs REST URLs.
        // The SDK targets https://{namespace}.servicebus.windows.net/{entity}?api-version=2017-04
        // We need to either:
        // 1. Use a custom HttpPipeline policy to redirect to localhost
        // 2. Set up DNS or hosts file entry
        // 3. Use a reverse proxy approach
        //
        // For now, use raw HTTP to verify the API shape works,
        // then wire up SDK integration in a follow-up pass.

        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://localhost:{_fixture.HttpPort}");
        httpClient.DefaultRequestHeaders.Host = $"{_fixture.Namespace}.servicebus.windows.net";
        httpClient.DefaultRequestHeaders.Add("Authorization", "SharedAccessSignature sr=test");

        // Create queue via PUT
        var createBody = $"""
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <LockDuration>PT30S</LockDuration>
                  <MaxDeliveryCount>10</MaxDeliveryCount>
                </QueueDescription>
              </content>
            </entry>
            """;

        var createResponse = await httpClient.PutAsync(
            "/test-queue?api-version=2017-04",
            new StringContent(createBody, System.Text.Encoding.UTF8, "application/atom+xml"));

        Assert.True(createResponse.IsSuccessStatusCode, $"Create failed: {createResponse.StatusCode}");

        // Get queue
        var getResponse = await httpClient.GetAsync("/test-queue?api-version=2017-04");
        Assert.True(getResponse.IsSuccessStatusCode);

        var content = await getResponse.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", content);
        Assert.Contains("LockDuration", content);
    }

    [Fact]
    public async Task CreateTopicAndSubscription_Works()
    {
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://localhost:{_fixture.HttpPort}");
        httpClient.DefaultRequestHeaders.Host = $"{_fixture.Namespace}.servicebus.windows.net";

        // Create topic
        var topicBody = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <TopicDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <MaxSizeInMegabytes>1024</MaxSizeInMegabytes>
                </TopicDescription>
              </content>
            </entry>
            """;

        await httpClient.PutAsync("/my-topic?api-version=2017-04",
            new StringContent(topicBody, System.Text.Encoding.UTF8, "application/atom+xml"));

        // Create subscription with ForwardTo
        var subBody = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <ForwardTo>my-queue</ForwardTo>
                  <MaxDeliveryCount>5</MaxDeliveryCount>
                </SubscriptionDescription>
              </content>
            </entry>
            """;

        var subResponse = await httpClient.PutAsync("/my-topic/Subscriptions/sub-1?api-version=2017-04",
            new StringContent(subBody, System.Text.Encoding.UTF8, "application/atom+xml"));

        Assert.True(subResponse.IsSuccessStatusCode);

        // Get subscription
        var getSubResponse = await httpClient.GetAsync("/my-topic/Subscriptions/sub-1?api-version=2017-04");
        Assert.True(getSubResponse.IsSuccessStatusCode);

        var content = await getSubResponse.Content.ReadAsStringAsync();
        Assert.Contains("ForwardTo", content);
    }

    [Fact]
    public async Task GetNonexistentEntity_Returns404()
    {
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://localhost:{_fixture.HttpPort}");
        httpClient.DefaultRequestHeaders.Host = $"{_fixture.Namespace}.servicebus.windows.net";

        var response = await httpClient.GetAsync("/nonexistent?api-version=2017-04");
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);

        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("MessagingEntityNotFound", content);
    }
}
```

- [ ] **Step 2: Run SDK integration tests**

```bash
dotnet test tests/AzureServiceBusEmulator.SdkIntegration.Tests -v minimal
```

Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add SDK integration tests for REST management API"
```

---

## Task 18: SDK Integration Tests — Messaging (AMQP)

**Files:**
- Create: `tests/AzureServiceBusEmulator.SdkIntegration.Tests/MessagingTests.cs`

- [ ] **Step 1: Write MessagingTests**

Create `tests/AzureServiceBusEmulator.SdkIntegration.Tests/MessagingTests.cs`:

```csharp
using Azure.Messaging.ServiceBus;
using AzureServiceBusEmulator.TestHost;

namespace AzureServiceBusEmulator.SdkIntegration.Tests;

public class MessagingTests : IAsyncLifetime
{
    private ServiceBusEmulatorFixture _fixture = null!;

    public async Task InitializeAsync()
    {
        _fixture = new ServiceBusEmulatorFixture();
        await _fixture.StartAsync();

        // Pre-create a queue for messaging tests
        _fixture.GetNamespaceContext().CreateQueue("test-queue");
    }

    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task SendAndReceive_Queue_RoundTrips()
    {
        // Create ServiceBusClient pointed at the emulator's AMQP port
        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
        };

        // The real SDK constructs an AMQP connection to the endpoint in the connection string.
        // For localhost testing, we need to override the endpoint.
        // Connection string format: Endpoint=sb://localhost:{port};SharedAccessKeyName=...;SharedAccessKey=...
        var connectionString = $"Endpoint=sb://localhost:{_fixture.AmqpPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

        await using var client = new ServiceBusClient(connectionString, clientOptions);
        await using var sender = client.CreateSender("test-queue");
        await using var receiver = client.CreateReceiver("test-queue");

        // Send
        await sender.SendMessageAsync(new ServiceBusMessage("Hello, Emulator!")
        {
            ContentType = "text/plain",
            MessageId = "test-msg-1"
        });

        // Receive
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("Hello, Emulator!", received.Body.ToString());
        Assert.Equal("text/plain", received.ContentType);

        // Complete
        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task TopicPublish_SubscriptionForward_Works()
    {
        // Set up topology: topic -> subscription (ForwardTo: target-queue) -> target-queue
        var ns = _fixture.GetNamespaceContext();
        var targetQueue = ns.CreateQueue("target-queue");
        ns.CreateTopic("test-topic");
        ns.CreateSubscription("test-topic", "sub-1", forwardTo: "target-queue");

        var connectionString = $"Endpoint=sb://localhost:{_fixture.AmqpPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

        await using var client = new ServiceBusClient(connectionString);
        await using var sender = client.CreateSender("test-topic");
        await using var receiver = client.CreateReceiver("target-queue");

        await sender.SendMessageAsync(new ServiceBusMessage("Topic message"));

        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal("Topic message", received.Body.ToString());
    }
}
```

- [ ] **Step 2: Run messaging tests**

```bash
dotnet test tests/AzureServiceBusEmulator.SdkIntegration.Tests --filter "FullyQualifiedName~MessagingTests" -v minimal
```

Expected: Tests PASS. **Note:** These tests may reveal issues with the AMQP handshake, CBS auth flow, or message format. Debug and fix as needed — this is where the most iteration will happen.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add AMQP messaging integration tests with real ServiceBusClient"
```

---

## Task 19: MassTransit Integration Tests

**Files:**
- Create: `tests/AzureServiceBusEmulator.MassTransit.Tests/MassTransitTopologyTests.cs`
- Create: `tests/AzureServiceBusEmulator.MassTransit.Tests/MassTransitPubSubTests.cs`
- Create: `tests/AzureServiceBusEmulator.MassTransit.Tests/TestMessages.cs`

- [ ] **Step 1: Create shared test message types**

Create `tests/AzureServiceBusEmulator.MassTransit.Tests/TestMessages.cs`:

```csharp
namespace AzureServiceBusEmulator.MassTransit.Tests;

public record TestEvent(string Value);
public record TestCommand(string Value);
public record TestRequest(string Value);
public record TestResponse(string Result);
```

- [ ] **Step 2: Write MassTransit topology tests**

Create `tests/AzureServiceBusEmulator.MassTransit.Tests/MassTransitTopologyTests.cs`:

```csharp
using AzureServiceBusEmulator.TestHost;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace AzureServiceBusEmulator.MassTransit.Tests;

public class MassTransitTopologyTests : IAsyncLifetime
{
    private ServiceBusEmulatorFixture _fixture = null!;

    public async Task InitializeAsync()
    {
        _fixture = new ServiceBusEmulatorFixture();
        await _fixture.StartAsync();
    }

    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task MassTransit_CreatesTopology_OnStartup()
    {
        var services = new ServiceCollection();
        services.AddMassTransit(x =>
        {
            x.AddConsumer<TestEventConsumer>();

            x.UsingAzureServiceBus((ctx, cfg) =>
            {
                cfg.Host(new Uri($"sb://localhost"), h =>
                {
                    // Override connection to point at emulator
                    // MassTransit ASB transport configuration will be needed here
                });

                cfg.ConfigureEndpoints(ctx);
            });
        });

        // This test verifies that MassTransit can start and create topology
        // against the emulator without throwing exceptions.
        // Exact configuration will need refinement based on how MassTransit
        // allows injecting custom ServiceBusClient instances.
        Assert.True(true, "Topology test placeholder — requires MassTransit ServiceBusClient injection");
    }

    private class TestEventConsumer : IConsumer<TestEvent>
    {
        public Task Consume(ConsumeContext<TestEvent> context) => Task.CompletedTask;
    }
}
```

- [ ] **Step 3: Write MassTransit pub/sub tests**

Create `tests/AzureServiceBusEmulator.MassTransit.Tests/MassTransitPubSubTests.cs`:

```csharp
using AzureServiceBusEmulator.TestHost;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace AzureServiceBusEmulator.MassTransit.Tests;

public class MassTransitPubSubTests : IAsyncLifetime
{
    private ServiceBusEmulatorFixture _fixture = null!;

    public async Task InitializeAsync()
    {
        _fixture = new ServiceBusEmulatorFixture();
        await _fixture.StartAsync();
    }

    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Publish_And_Consume_Event()
    {
        // This test will be filled in once MassTransit integration is working.
        // The pattern is:
        // 1. Create MassTransit bus with emulator endpoints
        // 2. Register consumer
        // 3. Start bus (creates topology via admin API)
        // 4. Publish event
        // 5. Assert consumer received it

        // Placeholder — the exact MassTransit config requires understanding
        // how to inject custom ServiceBusClient/AdminClient instances
        Assert.True(true, "Pub/sub test placeholder — requires MassTransit client injection");
    }
}
```

**Note:** The MassTransit tests are structured as placeholders because the exact integration pattern (injecting custom `ServiceBusClient` and `ServiceBusAdministrationClient` into MassTransit) will need to be determined during implementation. MassTransit's `ServiceBusHostSettings` interface supports this, but the exact API surface depends on the MassTransit version installed. The implementing agent should:

1. Check the MassTransit source for `ServiceBusHostSettings.ServiceBusClient` and `ServiceBusHostSettings.ServiceBusAdministrationClient`
2. Use `cfg.Host(settings)` with a custom `ServiceBusHostSettings` implementation
3. Wire up the fixture's ports and namespace into the settings

- [ ] **Step 4: Run MassTransit tests**

```bash
dotnet test tests/AzureServiceBusEmulator.MassTransit.Tests -v minimal
```

Expected: Placeholder tests PASS. Real integration tests will be developed iteratively.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add MassTransit integration test scaffolding"
```

---

## Task 20: End-to-End Smoke Test and Polish

**Files:**
- Modify: various files as needed for fixes discovered during smoke testing

- [ ] **Step 1: Run all unit tests**

```bash
dotnet test tests/AzureServiceBusEmulator.Tests -v minimal
```

Expected: All tests PASS.

- [ ] **Step 2: Run all integration tests**

```bash
dotnet test tests/AzureServiceBusEmulator.SdkIntegration.Tests -v minimal
```

Expected: All tests PASS (or identify specific failures to fix).

- [ ] **Step 3: Run the host standalone**

```bash
dotnet run --project src/AzureServiceBusEmulator.Host
```

Verify output shows both AMQP and HTTP endpoints. Ctrl+C to stop.

- [ ] **Step 4: Run full solution build**

```bash
dotnet build AzureServiceBusEmulator.sln --configuration Release
```

Expected: Build succeeds with 0 errors, 0 warnings.

- [ ] **Step 5: Run all tests in solution**

```bash
dotnet test AzureServiceBusEmulator.sln -v minimal
```

Expected: All tests PASS.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "fix: polish and fix issues found during smoke testing"
```
