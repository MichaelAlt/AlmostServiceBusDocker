# Dashboard Backend API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add JSON REST endpoints and SSE streaming for the dashboard UI to browse namespaces, entities, peek messages, and observe real-time message flow.

**Architecture:** Instrument the broker core with message counts, peek support, and a message event bus. Expose these via `/api/dashboard/` JSON endpoints on the existing Kestrel HTTP server. Real-time events use Server-Sent Events (SSE) at `/api/dashboard/events`.

**Tech Stack:** ASP.NET Core minimal APIs, System.Threading.Channels, Server-Sent Events

**Spec:** `docs/superpowers/specs/2026-04-02-dashboard-ui-design.md`

---

### Task 1: Broker instrumentation — namespace listing and message counts

**Files:**
- Modify: `src/AlmostServiceBus.Core/Broker/NamespaceRegistry.cs`
- Modify: `src/AlmostServiceBus.Core/Broker/QueueEntity.cs`

The dashboard needs to list namespaces and get message counts. Currently `NamespaceRegistry` has no `ListNamespaces()` and `QueueEntity` has no count tracking (Channel\<T\> doesn't expose count).

- [ ] **Step 1: Add ListNamespaces to NamespaceRegistry**

```csharp
// In NamespaceRegistry.cs, add:
public IReadOnlyCollection<string> ListNamespaces()
{
    return _namespaces.Keys.ToList().AsReadOnly();
}
```

- [ ] **Step 2: Add message count tracking to QueueEntity**

Add an `int _messageCount` field with `Interlocked` increment/decrement. Increment in `Enqueue()`, decrement in `TryDequeueImmediate()` and `DequeueAsync()`.

```csharp
// In QueueEntity.cs, add field:
private int _messageCount;

// Add public property:
public int MessageCount => _messageCount;

// In Enqueue(), after _channel.Writer.TryWrite(message):
Interlocked.Increment(ref _messageCount);

// In TryDequeueImmediate(), after _channel.Reader.TryRead(out var message):
Interlocked.Decrement(ref _messageCount);

// In DequeueAsync(), after reading from channel:
Interlocked.Decrement(ref _messageCount);
```

- [ ] **Step 3: Add PeekMessages to QueueEntity**

Channel\<T\> doesn't support peeking. Add a concurrent list that shadows the channel for peek access. Messages are added on enqueue and removed on complete/dead-letter.

```csharp
// In QueueEntity.cs, add field:
private readonly ConcurrentQueue<BrokeredMessage> _peekBuffer = new();

// In Enqueue(), also add to peek buffer:
_peekBuffer.Enqueue(message);

// In Complete(), remove from peek buffer:
// (ConcurrentQueue doesn't support removal — use a ConcurrentDictionary instead)
```

Actually, a simpler approach: use the existing `_pending` dictionary (messages that have been dequeued and are locked) plus a snapshot of the channel. For peek, maintain a `ConcurrentDictionary<string, BrokeredMessage> _allMessages` keyed by LockToken, added on Enqueue, removed on Complete/DeadLetter:

```csharp
// In QueueEntity.cs, add field:
private readonly ConcurrentDictionary<string, BrokeredMessage> _allMessages = new();

// In Enqueue():
_allMessages[message.LockToken!] = message;

// In Complete():
_allMessages.TryRemove(lockToken, out _);

// In DeadLetter(string lockToken, ...):
_allMessages.TryRemove(lockToken, out _);

// Public peek method:
public IReadOnlyList<BrokeredMessage> PeekMessages(int maxCount = 50)
{
    return _allMessages.Values
        .OrderBy(m => m.SequenceNumber)
        .Take(maxCount)
        .ToList()
        .AsReadOnly();
}
```

- [ ] **Step 4: Run existing tests to verify no regressions**

Run: `dotnet test AlmostServiceBus.sln --verbosity quiet`

Expected: All 148 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/AlmostServiceBus.Core/Broker/NamespaceRegistry.cs src/AlmostServiceBus.Core/Broker/QueueEntity.cs
git commit -m "feat: add namespace listing, message counts, and peek to broker"
```

---

### Task 2: Message event bus for SSE

**Files:**
- Create: `src/AlmostServiceBus.Core/Broker/MessageEventBus.cs`
- Modify: `src/AlmostServiceBus.Core/Broker/QueueEntity.cs`

A pub/sub event bus that the broker publishes to when messages are enqueued, completed, or dead-lettered. The SSE endpoint subscribes to this bus.

- [ ] **Step 1: Create MessageEventBus**

```csharp
// src/AlmostServiceBus.Core/Broker/MessageEventBus.cs
using System.Threading.Channels;

namespace AlmostServiceBus.Core.Broker;

public enum MessageEventType
{
    Enqueued,
    Completed,
    DeadLettered,
    Abandoned
}

public record MessageEvent(
    MessageEventType Type,
    string Namespace,
    string Entity,
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? BodyPreview,
    Dictionary<string, object>? ScalarProperties,
    DateTimeOffset Timestamp);

public class MessageEventBus
{
    private readonly List<Channel<MessageEvent>> _subscribers = [];
    private readonly Lock _lock = new();

    public ChannelReader<MessageEvent> Subscribe()
    {
        var channel = Channel.CreateBounded<MessageEvent>(
            new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.DropOldest });
        lock (_lock)
        {
            _subscribers.Add(channel);
        }
        return channel.Reader;
    }

    public void Unsubscribe(ChannelReader<MessageEvent> reader)
    {
        lock (_lock)
        {
            _subscribers.RemoveAll(c => c.Reader == reader);
        }
    }

    public void Publish(MessageEvent evt)
    {
        lock (_lock)
        {
            foreach (var channel in _subscribers)
            {
                channel.Writer.TryWrite(evt);
            }
        }
    }
}
```

- [ ] **Step 2: Wire MessageEventBus into QueueEntity**

Pass `MessageEventBus` and namespace name into `QueueEntity`. Publish events on Enqueue, Complete, DeadLetter, Abandon.

The `QueueEntity` constructor needs to accept the event bus optionally (to avoid breaking existing code). Add a `SetEventBus(MessageEventBus bus, string namespaceName, string entityName)` method instead, called by NamespaceContext after creation.

```csharp
// In QueueEntity.cs, add fields:
private MessageEventBus? _eventBus;
private string? _namespaceName;
private string? _entityName;

public void SetEventBus(MessageEventBus bus, string namespaceName, string entityName)
{
    _eventBus = bus;
    _namespaceName = namespaceName;
    _entityName = entityName;
}

// In Enqueue(), after adding to channel:
_eventBus?.Publish(new MessageEvent(
    MessageEventType.Enqueued, _namespaceName ?? "", _entityName ?? "",
    message.MessageId, message.SequenceNumber, message.ContentType,
    TruncateBody(message), ExtractScalars(message),
    DateTimeOffset.UtcNow));

// Helper methods:
private static string? TruncateBody(BrokeredMessage message)
{
    if (message.Body is null || message.Body.Length == 0) return null;
    var text = System.Text.Encoding.UTF8.GetString(message.Body);
    return text.Length > 500 ? text[..500] : text;
}

private static Dictionary<string, object>? ExtractScalars(BrokeredMessage message)
{
    // Try to parse JSON body and extract scalar properties from the MassTransit "message" envelope
    try
    {
        var doc = System.Text.Json.JsonDocument.Parse(message.Body);
        var root = doc.RootElement;
        // MassTransit wraps in { "message": { ... } }
        if (root.TryGetProperty("message", out var inner))
            root = inner;
        var scalars = new Dictionary<string, object>();
        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.String)
                scalars[prop.Name] = prop.Value.GetString()!;
            else if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.Number)
                scalars[prop.Name] = prop.Value.GetDouble();
            else if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False)
                scalars[prop.Name] = prop.Value.GetBoolean();
            if (scalars.Count >= 5) break;
        }
        return scalars.Count > 0 ? scalars : null;
    }
    catch { return null; }
}
```

- [ ] **Step 3: Wire MessageEventBus into NamespaceContext**

Pass the event bus through NamespaceRegistry → NamespaceContext → QueueEntity. Add it to NamespaceRegistry as an optional dependency, and NamespaceContext calls `SetEventBus()` when creating queues.

```csharp
// In NamespaceRegistry.cs, add field and constructor param:
private readonly MessageEventBus? _eventBus;

public NamespaceRegistry(MessageEventBus? eventBus = null)
{
    _eventBus = eventBus;
}

// In GetOrCreate(), pass event bus to NamespaceContext:
// NamespaceContext constructor needs to accept it too
```

```csharp
// In NamespaceContext.cs, add field:
private readonly MessageEventBus? _eventBus;

// In CreateQueue():
// After creating the queue, call queue.SetEventBus(_eventBus, Name, name);
```

- [ ] **Step 4: Run tests**

Run: `dotnet test AlmostServiceBus.sln --verbosity quiet`

Expected: All tests pass. Existing code creates `NamespaceRegistry()` with no args which defaults event bus to null.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add MessageEventBus for real-time dashboard events"
```

---

### Task 3: Dashboard JSON API endpoints

**Files:**
- Create: `src/AlmostServiceBus.Core/Dashboard/DashboardApiEndpoints.cs`
- Create: `src/AlmostServiceBus.Core/Dashboard/DashboardModels.cs`

REST endpoints under `/api/dashboard/` that return JSON. These read from the broker's in-memory state.

- [ ] **Step 1: Create response models**

```csharp
// src/AlmostServiceBus.Core/Dashboard/DashboardModels.cs
namespace AlmostServiceBus.Core.Dashboard;

public record NamespaceInfo(string Name, int QueueCount, int TopicCount);

public record EntityOverview(
    List<QueueInfo> Queues,
    List<TopicInfo> Topics);

public record QueueInfo(
    string Name,
    int MessageCount,
    int DeadLetterCount,
    int MaxDeliveryCount,
    string? ForwardTo);

public record TopicInfo(
    string Name,
    List<SubscriptionInfo> Subscriptions);

public record SubscriptionInfo(
    string Name,
    string? ForwardTo,
    int MessageCount,
    int RuleCount);

public record MessageInfo(
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? CorrelationId,
    int DeliveryCount,
    DateTimeOffset EnqueuedTimeUtc,
    string? Subject,
    Dictionary<string, object>? ApplicationProperties,
    string? BodyText,
    Dictionary<string, object>? ScalarProperties);
```

- [ ] **Step 2: Create DashboardApiEndpoints**

```csharp
// src/AlmostServiceBus.Core/Dashboard/DashboardApiEndpoints.cs
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Core.Dashboard;

public static class DashboardApiEndpoints
{
    public static IEndpointRouteBuilder MapDashboardApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry)
    {
        var api = app.MapGroup("/api/dashboard");

        api.MapGet("/namespaces", () =>
        {
            return registry.ListNamespaces().Select(name =>
            {
                var ns = registry.Get(name);
                return new NamespaceInfo(
                    name,
                    ns?.GetQueues().Count ?? 0,
                    ns?.GetTopics().Count ?? 0);
            }).ToList();
        });

        api.MapGet("/namespaces/{ns}/entities", (string ns) =>
        {
            var context = registry.Get(ns);
            if (context is null) return Results.NotFound();

            var queues = context.GetQueues().Select(q => new QueueInfo(
                q.Name, q.MessageCount, q.DeadLetterQueue.MessageCount,
                q.MaxDeliveryCount, q.ForwardTo)).ToList();

            var topics = context.GetTopics().Select(t => new TopicInfo(
                t.Name,
                t.GetSubscriptions().Select(s => new SubscriptionInfo(
                    s.Name, s.ForwardTo,
                    s.Queue.MessageCount,
                    s.GetRules().Count)).ToList()
            )).ToList();

            return Results.Ok(new EntityOverview(queues, topics));
        });

        api.MapGet("/namespaces/{ns}/queues/{**queueName}/messages", (string ns, string queueName) =>
        {
            var context = registry.Get(ns);
            var queue = context?.GetQueue(queueName);
            if (queue is null) return Results.NotFound();

            return Results.Ok(queue.PeekMessages(50).Select(ToMessageInfo).ToList());
        });

        api.MapGet("/namespaces/{ns}/queues/{**queueName}/deadletter", (string ns, string queueName) =>
        {
            var context = registry.Get(ns);
            var queue = context?.GetQueue(queueName);
            if (queue is null) return Results.NotFound();

            return Results.Ok(queue.DeadLetterQueue.PeekMessages(50).Select(ToMessageInfo).ToList());
        });

        api.MapDelete("/namespaces/{ns}/queues/{**queueName}/messages", (string ns, string queueName) =>
        {
            var context = registry.Get(ns);
            var queue = context?.GetQueue(queueName);
            if (queue is null) return Results.NotFound();

            // Purge: drain all messages
            while (queue.TryDequeueImmediate() is not null) { }
            return Results.Ok();
        });

        api.MapDelete("/namespaces/{ns}/queues/{**queueName}/deadletter", (string ns, string queueName) =>
        {
            var context = registry.Get(ns);
            var queue = context?.GetQueue(queueName);
            if (queue is null) return Results.NotFound();

            while (queue.DeadLetterQueue.TryDequeueImmediate() is not null) { }
            return Results.Ok();
        });

        return app;
    }

    private static MessageInfo ToMessageInfo(BrokeredMessage m)
    {
        string? bodyText = null;
        Dictionary<string, object>? scalars = null;
        if (m.Body is { Length: > 0 })
        {
            bodyText = System.Text.Encoding.UTF8.GetString(m.Body);
            scalars = ExtractScalars(bodyText);
        }

        return new MessageInfo(
            m.MessageId, m.SequenceNumber, m.ContentType,
            m.CorrelationId, m.DeliveryCount, m.EnqueuedTimeUtc,
            m.Subject, m.ApplicationProperties, bodyText, scalars);
    }

    private static Dictionary<string, object>? ExtractScalars(string json)
    {
        try
        {
            var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("message", out var inner)) root = inner;
            var result = new Dictionary<string, object>();
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.String)
                    result[prop.Name] = prop.Value.GetString()!;
                else if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.Number)
                    result[prop.Name] = prop.Value.GetDouble();
                else if (prop.Value.ValueKind is System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False)
                    result[prop.Name] = prop.Value.GetBoolean();
                if (result.Count >= 5) break;
            }
            return result.Count > 0 ? result : null;
        }
        catch { return null; }
    }
}
```

- [ ] **Step 3: Run tests**

Run: `dotnet test AlmostServiceBus.sln --verbosity quiet`

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add dashboard JSON API endpoints"
```

---

### Task 4: SSE endpoint for real-time events

**Files:**
- Create: `src/AlmostServiceBus.Core/Dashboard/DashboardSseEndpoint.cs`

SSE endpoint at `/api/dashboard/events` that streams `MessageEvent` objects as `text/event-stream`. Client can filter by namespace and entity via query params.

- [ ] **Step 1: Create SSE endpoint**

```csharp
// src/AlmostServiceBus.Core/Dashboard/DashboardSseEndpoint.cs
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Core.Dashboard;

public static class DashboardSseEndpoint
{
    public static IEndpointRouteBuilder MapDashboardSse(
        this IEndpointRouteBuilder app,
        MessageEventBus eventBus)
    {
        app.MapGet("/api/dashboard/events", async (HttpContext httpContext, string? ns, string? entity) =>
        {
            httpContext.Response.ContentType = "text/event-stream";
            httpContext.Response.Headers.CacheControl = "no-cache";
            httpContext.Response.Headers.Connection = "keep-alive";

            var reader = eventBus.Subscribe();
            var ct = httpContext.RequestAborted;

            try
            {
                await foreach (var evt in reader.ReadAllAsync(ct))
                {
                    // Filter by namespace and entity if specified
                    if (ns is not null && !evt.Namespace.Equals(ns, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (entity is not null && !evt.Entity.Equals(entity, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var json = JsonSerializer.Serialize(evt, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    });

                    await httpContext.Response.WriteAsync($"data: {json}\n\n", ct);
                    await httpContext.Response.Body.FlushAsync(ct);
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                eventBus.Unsubscribe(reader);
            }
        });

        return app;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "feat: add SSE endpoint for real-time dashboard events"
```

---

### Task 5: Wire up dashboard API in Host and TestHost

**Files:**
- Modify: `src/AlmostServiceBus.Host/Program.cs`
- Modify: `src/AlmostServiceBus.TestHost/ServiceBusEmulatorFixture.cs`

Register the dashboard endpoints and the message event bus in both the standalone host and the test fixture.

- [ ] **Step 1: Update Host Program.cs**

Add after `app.MapServiceBusManagementApi(registry)`:

```csharp
app.MapDashboardApi(registry);
app.MapDashboardSse(eventBus);
```

Create `MessageEventBus` and pass it to `NamespaceRegistry`:

```csharp
var eventBus = new MessageEventBus();
var registry = new NamespaceRegistry(eventBus);
```

Add using:
```csharp
using AlmostServiceBus.Core.Dashboard;
```

- [ ] **Step 2: Update TestHost fixture similarly**

Add event bus to the fixture's NamespaceRegistry creation.

- [ ] **Step 3: Enable CORS for dashboard dev**

The Vue dev server runs on a different port than Kestrel. Add CORS middleware:

```csharp
// In Program.cs, before app.MapServiceBusManagementApi:
app.UseCors(policy => policy
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader());
```

- [ ] **Step 4: Run full test suite**

Run: `dotnet test AlmostServiceBus.sln --verbosity quiet`

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: wire up dashboard API and SSE in Host and TestHost"
```

---

### Task 6: Manual smoke test

- [ ] **Step 1: Start the emulator**

Run: `dotnet run --project src/AlmostServiceBus.Host`

- [ ] **Step 2: Create some entities via curl**

```bash
# Create a queue
curl -k -X PUT https://localhost:5672/test-queue \
  -H "Host: localhost" \
  -H "Content-Type: application/atom+xml" \
  -d '<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><MaxDeliveryCount>10</MaxDeliveryCount></QueueDescription></content></entry>'

# List namespaces
curl http://localhost:{HTTP_PORT}/api/dashboard/namespaces

# List entities
curl http://localhost:{HTTP_PORT}/api/dashboard/namespaces/default/entities
```

- [ ] **Step 3: Test SSE endpoint**

```bash
curl -N http://localhost:{HTTP_PORT}/api/dashboard/events
```

Send a message to the queue and verify the SSE stream outputs the event.

- [ ] **Step 4: Commit any fixes**
