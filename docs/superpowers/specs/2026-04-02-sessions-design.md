# Sessions — Design Spec

## Goal

Add AMQP session support to the emulator so queues with `RequiresSession = true` partition messages by `SessionId`, deliver them in FIFO order per session, and support session locking + session state.

## Motivation

Sessions are required by:
- Wolverine: 3 failing tests (FIFO queues, session identifiers)
- MassTransit: saga state machines use session state
- NServiceBus: ordered processing patterns

---

## Broker Layer

### SessionManager (new class)

Each session-enabled `QueueEntity` gets a `SessionManager` that tracks:

```
SessionManager
├── _sessions: ConcurrentDictionary<string, SessionState>
│   └── SessionState
│       ├── SessionId: string
│       ├── Messages: Channel<BrokeredMessage>  (FIFO per session)
│       ├── LockedBy: string? (receiver link ID)
│       ├── LockedUntil: DateTimeOffset
│       └── State: byte[]? (arbitrary user state)
└── Methods
    ├── Enqueue(message) → routes to session by SessionId
    ├── TryAcceptSession(sessionId?) → locks session, returns SessionState
    ├── ReleaseSession(sessionId)
    ├── RenewSessionLock(sessionId) → extends LockedUntil
    ├── GetSessionState(sessionId) → byte[]?
    ├── SetSessionState(sessionId, byte[]?)
    └── GetAvailableSessionIds() → sessions with messages
```

### QueueEntity changes

When `RequiresSession` is true:
- `Enqueue(message)` delegates to `SessionManager.Enqueue` which routes by `message.SessionId`
- Messages without a `SessionId` are rejected (real ASB does this)
- `TryDequeueImmediate()` is NOT used — session receivers dequeue from their locked session directly
- `DequeueAsync()` same — not used for session queues

When `RequiresSession` is false: no changes, everything works as before.

### SessionState

```csharp
public class SessionState
{
    public string SessionId { get; }
    public Channel<BrokeredMessage> Messages { get; }
    public string? LockedBy { get; set; }
    public DateTimeOffset LockedUntil { get; set; }
    public byte[]? UserState { get; set; }
    public int MessageCount { get; }
}
```

Sessions are created lazily when a message with a new `SessionId` arrives. They're never deleted (matching real ASB behavior — sessions persist until the queue is deleted).

---

## AMQP Layer

### Session receiver link detection

When a client opens a receiver link, the `Attach` frame's `Source` may contain a filter map:
```
source.FilterSet["com.microsoft:session-filter"] = sessionId or null
```

- `sessionId` specified: lock that specific session
- `null`: lock the next available session (one with messages)

### ReceiverLinkEndpoint changes

Add a `SessionReceiverLinkEndpoint` (or a mode flag on `ReceiverLinkEndpoint`):

- On attach: accept the session based on the filter
  - If specific session requested → lock it
  - If null (next available) → find a session with messages, lock it
  - If no sessions available → detach with `com.microsoft:timeout` error
- Send `x-opt-session-id` in the first transfer's message annotations (the SDK reads this to determine which session was locked)
- Pump messages only from the locked session's channel
- On link close: release the session lock

### ServiceBusLinkProcessor changes

When creating a receiver link, check if the queue requires sessions and if the source has a session filter. If so, create a session-aware receiver instead of the normal `ReceiverLinkEndpoint`.

### ManagementLinkEndpoint additions

Handle these operations on the `$management` link:
- `com.microsoft:renew-session-lock` — extend session lock
- `com.microsoft:get-session-state` — return session state bytes
- `com.microsoft:set-session-state` — store session state bytes

---

## Management API

### Queue creation

`RequiresSession` is already parsed from the Atom XML and stored on `QueueEntity`. No changes needed for creation.

### Subscription creation

Subscriptions can also require sessions. Same `RequiresSession` flag on `SubscriptionEntity`. When the subscription's queue (`sub.Queue`) requires sessions, the same session logic applies.

---

## What stays the same

- Non-session queues: completely unchanged
- Topic publishing: unchanged (messages are cloned to subscriptions as before)
- REST management API: unchanged (RequiresSession already round-trips)
- Dashboard: works with existing peek/count mechanisms
- TLS multiplexer: unchanged
- SASL/CBS: unchanged

---

## Testing

### Conformance tests to add

1. **Session send and receive** — send 3 messages with same SessionId, receive all 3 in order from a session receiver
2. **Multiple sessions** — send messages to 2 different sessions, verify each session receiver gets only its messages
3. **Session state** — set session state, get session state, verify round-trip
4. **Next available session** — send to 2 sessions, open receiver with null filter, verify one session is locked

### Framework tests that should pass after

- Wolverine: `send_and_receive_multiple_messages_to_queue_with_session_identifier`
- Wolverine: `send_and_receive_multiple_messages_to_subscription_with_session_identifier`
- Wolverine: `split_messages_with_different_sessionids_into_separate_batches`

---

## Scope

### In scope
- Session partitioning by SessionId
- Session locking (one receiver per session)
- Session lock renewal
- Session state (get/set bytes)
- Next available session
- Session filter on AMQP receiver links
- Conformance tests

### Out of scope
- Session lock expiry with automatic release (nice to have, not blocking)
- Maximum concurrent sessions limit
- Session-enabled subscriptions (can add later if needed)
