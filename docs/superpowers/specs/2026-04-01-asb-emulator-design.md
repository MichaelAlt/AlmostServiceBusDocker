# Azure Service Bus Emulator — Design Spec

## Goal

Build a MassTransit-compatible Azure Service Bus emulator that is 1:1 compatible with the real `Azure.Messaging.ServiceBus` SDK. The emulator supports both the AMQP 1.0 messaging protocol and the REST management API, allowing MassTransit to create its full topology (queues, topics, subscriptions, rules) and send/receive messages without any code changes in the consuming application.

## Motivation

- **Local dev/CI testing** without a real Azure Service Bus namespace
- Microsoft's official emulator (preview) does not support the management API, which MassTransit is wholly reliant on for topology creation
- Need something lightweight that can run as an in-process library (test fixture) or as a standalone container (Aspire)

## Approach

**AMQPNetLite + ASP.NET Core.** Use AMQPNetLite's `ContainerHost` as the AMQP 1.0 listener and ASP.NET Core for the REST management API. Both share a single in-memory broker core.

---

## Architecture

```
+---------------------------------------------------+
|                  Emulator Host                     |
|                                                    |
|  +----------------+       +--------------------+   |
|  |  AMQP 1.0      |       |  REST/HTTPS API    |   |
|  |  Listener       |       |  (ASP.NET Core)    |   |
|  |  (AMQPNetLite)  |       |  Admin operations  |   |
|  +--------+--------+       +---------+----------+   |
|           |                          |              |
|           +----------+---------------+              |
|                      v                              |
|  +------------------------------------------+      |
|  |            Broker Core                    |      |
|  |  +--------------------------------------+ |      |
|  |  |   Namespace Registry                 | |      |
|  |  |   (tenant isolation by URL)          | |      |
|  |  +--------------------------------------+ |      |
|  |  |   Queues (ConcurrentDict)            | |      |
|  |  |   Topics -> Subscriptions            | |      |
|  |  |   Rules / Filters                    | |      |
|  |  |   Message Store (Channels)           | |      |
|  |  +--------------------------------------+ |      |
|  +------------------------------------------+      |
+---------------------------------------------------+
```

### Two Protocol Surfaces

1. **AMQP 1.0** — `ServiceBusClient` connects here for sending/receiving messages. Includes CBS authentication on the `$cbs` node.
2. **REST/HTTPS** — `ServiceBusAdministrationClient` connects here for topology management (create/get/update/delete queues, topics, subscriptions, rules). Uses the Atom XML wire format.

### Tenant Isolation

The namespace is extracted from the connection URL. `sb://test-abc123.localhost` and `sb://test-def456.localhost` get completely separate entity stores. A `ConcurrentDictionary<string, NamespaceContext>` at the top level provides isolation. Each test run or app instance uses a random namespace identifier to get a clean slate.

---

## REST Management API

The `ServiceBusAdministrationClient` uses the Azure Service Bus Atom XML REST API. Endpoints follow this pattern:

```
GET/PUT/DELETE  https://{namespace}.servicebus.windows.net/{entityPath}?api-version=2017-04
```

Request/response bodies are Atom XML feeds with embedded description elements.

### Operations

| Operation | HTTP | Path |
|-----------|------|------|
| Create/Get Queue | PUT/GET | `/{queueName}` |
| Create/Get Topic | PUT/GET | `/{topicName}` |
| Create/Get Subscription | PUT/GET | `/{topicName}/Subscriptions/{subName}` |
| Update Subscription | PUT | `/{topicName}/Subscriptions/{subName}` (with `If-Match: *`) |
| Delete Subscription | DELETE | `/{topicName}/Subscriptions/{subName}` |
| Get Rule | GET | `/{topicName}/Subscriptions/{subName}/Rules/{ruleName}` |
| Get Rules (list) | GET | `/{topicName}/Subscriptions/{subName}/Rules` |
| Update Rule | PUT | `/{topicName}/Subscriptions/{subName}/Rules/{ruleName}` |

### Error Responses

MassTransit expects specific error responses:
- **404** with `MessagingEntityNotFound` sub-code — entity doesn't exist
- **409** with `MessagingEntityAlreadyExists` sub-code — entity already exists

MassTransit uses an idempotent create pattern: GET first, catch 404, then PUT, catch 409 and GET again.

### Authentication

The admin client sends a Bearer or SAS token in the `Authorization` header. The emulator accepts all tokens without validation.

---

## AMQP 1.0 Messaging Layer

Uses AMQPNetLite `ContainerHost` with `ILinkProcessor` for full control over link routing.

### Connection Flow

1. Client opens AMQP connection to `localhost:{port}`
2. Client attaches to `$cbs` node, sends `put-token` — emulator accepts all tokens, responds with `status-code: 200`
3. Client attaches sender/receiver links with target/source addresses
4. Emulator resolves address against the namespace's entity registry and creates the appropriate `LinkEndpoint`

### Link Types

| SDK Operation | AMQP Link | Address |
|---|---|---|
| Send to queue | Sender -> target | `{queueName}` |
| Send to topic | Sender -> target | `{topicName}` |
| Receive from queue | Receiver <- source | `{queueName}` |
| Receive from subscription | Receiver <- source | `{topicName}/Subscriptions/{subName}` |
| Schedule message | Sender -> target | `{entity}` (with `x-opt-scheduled-enqueue-time`) |
| Cancel scheduled | Request on `$management` | Operation: `com.microsoft:cancel-scheduled-message` |

### Message Routing

- **Queue**: Messages go into a `Channel<BrokeredMessage>` (bounded). One consumer gets each message (competing consumers).
- **Topic**: Messages are fanned out to all matching subscriptions. Each subscription has its own channel.
- **Subscription forwarding**: If `ForwardTo` is set, messages route to the target queue instead of the subscription's own channel. This is MassTransit's standard pattern — topics fan out to subscriptions that forward into consuming queues.
- **Rules/Filters**: `TrueFilter` matches everything (MassTransit's common case). `SqlFilter` and `CorrelationFilter` stubbed as match-all initially.

### Message Settlement

- **Complete** — remove message from the queue
- **Abandon** — return message to the head of the queue, increment delivery count
- **DeadLetter** — move to `{entity}/$deadletterqueue` (created lazily)

---

## Broker Core

### In-Memory State Model

```
NamespaceRegistry (ConcurrentDictionary<string, NamespaceContext>)
+-- NamespaceContext
    +-- Queues (ConcurrentDictionary<string, QueueEntity>)
    |   +-- QueueEntity
    |       +-- Properties (name, TTL, lock duration, max delivery count, etc.)
    |       +-- Messages (Channel<BrokeredMessage>)
    |       +-- DeadLetterQueue (Channel<BrokeredMessage>)
    +-- Topics (ConcurrentDictionary<string, TopicEntity>)
        +-- TopicEntity
            +-- Properties (name, TTL, duplicate detection, etc.)
            +-- Subscriptions (ConcurrentDictionary<string, SubscriptionEntity>)
                +-- SubscriptionEntity
                    +-- Properties (ForwardTo, max delivery count, etc.)
                    +-- Rules (ConcurrentDictionary<string, RuleEntity>)
                    +-- Messages (Channel<BrokeredMessage>) — only if no ForwardTo
```

### BrokeredMessage

- MessageId, Body, ContentType
- ApplicationProperties
- CorrelationId, SessionId, PartitionKey
- ScheduledEnqueueTimeUtc
- DeliveryCount
- SequenceNumber (namespace-wide incrementing long)

### Key Behaviors

- **Publish to topic**: Iterate all subscriptions, evaluate rules (TrueFilter for now), if `ForwardTo` is set route to that queue, otherwise enqueue in the subscription's own channel.
- **Scheduled messages**: Stored separately. A background `Task` with a `PeriodicTimer` checks for due messages and moves them into the live queue.
- **Dead-lettering**: After `MaxDeliveryCount` abandons, move to the dead-letter channel.
- **Sequence numbers**: Namespace-scoped `Interlocked.Increment` on a `long`.
- **No persistence**: Everything is in-memory. Emulator restart = clean state. This is a feature for testing.

---

## Hosting & Packaging

### Project Structure

```
AlmostServiceBus/
+-- src/
|   +-- AlmostServiceBus.Core/          # Class library
|   |   +-- Broker/                            # NamespaceRegistry, entities, message store
|   |   +-- Amqp/                              # ContainerHost, LinkProcessor, CbsHandler
|   |   +-- Management/                        # Atom XML serialization, REST route handlers
|   +-- AlmostServiceBus.Host/          # Console app / Generic Host
|   |   +-- Program.cs                         # Kestrel (REST) + AMQP listener startup
|   +-- AlmostServiceBus.TestHost/      # Test fixture library
|       +-- ServiceBusEmulatorFixture.cs        # Starts both listeners, provides connection string
+-- tests/
|   +-- AlmostServiceBus.Tests/         # Unit + integration tests
|   +-- AlmostServiceBus.MassTransit.Tests/  # MassTransit integration tests
+-- AlmostServiceBus.sln
```

### Three Packages

| Package | Purpose | Use case |
|---|---|---|
| `Core` | Broker + AMQP + REST handlers | Shared library |
| `Host` | Standalone console app / container | `docker run` or `dotnet run`, Aspire |
| `TestHost` | xUnit/NUnit fixture wrapper | `new ServiceBusEmulatorFixture()` in test setup |

### Test Fixture API

```csharp
public class MyTests : IAsyncLifetime
{
    private ServiceBusEmulatorFixture _emulator;

    public async Task InitializeAsync()
    {
        _emulator = new ServiceBusEmulatorFixture(); // random namespace
        await _emulator.StartAsync();
    }

    public async Task DisposeAsync() => await _emulator.StopAsync();

    // _emulator.ConnectionString -> connection string for ServiceBusClient
    // _emulator.AdminConnectionString -> connection string for ServiceBusAdministrationClient
    // _emulator.AmqpPort, _emulator.HttpPort
}
```

### Connection Strategy

- **Test fixture mode**: The fixture provides pre-configured `ServiceBusClient` and `ServiceBusAdministrationClient` instances pointed at localhost. MassTransit supports injecting pre-created clients via `ServiceBusHostSettings.ServiceBusClient` and `ServiceBusHostSettings.ServiceBusAdministrationClient`.
- **Standalone/container mode**: Use connection string with custom endpoint override (`CustomEndpointAddress` in `ServiceBusClientOptions`).

### Namespace Extraction

The emulator extracts the tenant namespace from the host portion of the connection URI. For AMQP connections, the `$cbs` put-token message contains the entity URI which includes the namespace. For REST, the `Host` header identifies the namespace. When running on localhost, the namespace is embedded in the hostname (e.g., `test-abc123.localhost`). The REST API uses path-based routing — all requests to the same HTTP port are disambiguated by the `Host` header, so a single Kestrel instance serves all namespaces.

---

## Scope

### In Scope (v1)

- AMQP 1.0 listener via AMQPNetLite ContainerHost
- CBS auth (accept everything)
- Send/receive on queues
- Publish to topics with fan-out to subscriptions
- Subscription forwarding (ForwardTo)
- REST management API (Atom XML) for create/get/update/delete of queues, topics, subscriptions, rules
- TrueFilter rules (match all)
- Message settlement (complete, abandon, dead-letter)
- Dead-letter queues
- Scheduled messages (enqueue with delay, cancel by sequence number)
- Namespace-level tenant isolation
- Competing consumers on queues
- Test fixture library with auto-start/stop
- Standalone host for container/Aspire use

### Out of Scope (later)

- Sessions / session processors
- Lock renewal timers
- Duplicate detection
- Partitioning
- SqlFilter / CorrelationFilter evaluation (stubbed as match-all)
- Message browsing (peek)
- Transactions
- AMQP WebSocket transport
- Max message size enforcement
- AutoDeleteOnIdle cleanup
- Message TTL expiry
- UI for namespace introspection (planned after core works)

### Key Risk

The Atom XML serialization for the management API. The `ServiceBusAdministrationClient` expects exact XML element names and namespaces. This is the most likely source of early friction and will require matching the format against the Azure SDK source or captured traffic.

---

## Test Plan

### 1. Unit Tests (`AlmostServiceBus.Tests`)

Test the broker core in isolation:
- Enqueue/dequeue on queues
- Topic fan-out to subscriptions
- Subscription forwarding (ForwardTo routes to target queue)
- Dead-lettering after MaxDeliveryCount
- Scheduled messages (enqueue with future time, verify delivery after due)
- Namespace isolation (two namespaces, verify no cross-contamination)
- Sequence number assignment

### 2. Management API Tests

Send real Atom XML HTTP requests to the REST API:
- Create queue, get queue, verify properties round-trip
- Create topic, get topic
- Create subscription with ForwardTo, get subscription
- Create/update/delete rules
- Verify 404 response for non-existent entities
- Verify 409 response for duplicate creation
- Update subscription properties, verify changes persisted

### 3. SDK Integration Tests

Use the real `ServiceBusClient` and `ServiceBusAdministrationClient` against the emulator (no MassTransit):
- Create entities via admin client
- Send message via ServiceBusClient, receive via ServiceBusClient
- Verify CBS authentication handshake completes
- Schedule a message, verify delayed delivery
- Cancel a scheduled message

### 4. MassTransit Integration Tests (`AlmostServiceBus.MassTransit.Tests`)

A test MassTransit application exercising real-world patterns:
- Configure MassTransit with `UseAzureServiceBus()` pointed at the emulator
- **Topology auto-creation on startup** — MT creates queues/topics/subscriptions via admin API
- **Publish/consume via topics** — standard MT pub/sub pattern
- **Send/consume via queues** — direct send
- **Request/response** — request client with response
- **Multiple consumers on the same topic** — verify independent delivery
- **Message serialization** — verify messages round-trip with correct content

### 5. Multi-Namespace Isolation Test

Spin up two MassTransit bus instances on different namespaces against the same emulator. Publish messages on one namespace, verify they do not appear on the other.
