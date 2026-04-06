# TCP Multiplexer — Transparent Single-Port DX

## Goal

Make the Azure Service Bus Emulator work with a plain connection string for both `ServiceBusClient` (AMQP) and `ServiceBusAdministrationClient` (HTTPS) on a single port, with no custom transport or client configuration required.

## Motivation

Currently the emulator runs AMQP on port 5672 and the REST management API on port 5300 over plain HTTP. The `ServiceBusAdministrationClient` constructs `https://{host}:{port}` URLs from the connection string's `Endpoint=sb://{host}:{port}`, so it sends HTTPS to the AMQP port — which fails. Users must use a custom `LocalRedirectTransport` to rewrite requests to the correct port and scheme.

This is poor DX. A real Azure Service Bus namespace works with a plain connection string. The emulator should too.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Single port multiplexing | TCP proxy with first-byte sniffing | Keeps ContainerHost and Kestrel untouched; ~50-80 lines of code |
| TLS certificate | ASP.NET dev cert (`dotnet dev-certs https`) | Zero-config for most .NET devs; well-known fallback command |
| AMQP transport | Plain AMQP only (no AMQP-over-TLS) | Matches what the .NET SDK sends to `sb://` endpoints |
| ContainerHost replacement | None — stays as-is behind the proxy | Avoids coupling to AMQPNetLite internals |

---

## Architecture

```
                    Client
                      |
              Port 5672 (public)
                      |
            +-------------------+
            |  TCP Multiplexer  |
            |  (peek 1st byte)  |
            +-------------------+
              |               |
         0x41 (AMQP)    0x16 (TLS)
              |               |
    +---------+----+   +------+--------+
    | ContainerHost|   |   Kestrel     |
    | (internal    |   |   (internal   |
    |  port, AMQP) |   |    port, HTTPS|
    +--------------+   |   Mgmt API)   |
              |        +---------------+
              |               |
              +-------+-------+
                      |
              NamespaceRegistry
              (shared in-memory)
```

### Connection String

Single connection string for both SDK clients:

```
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator
```

- `ServiceBusClient` sends plain AMQP to port 5672 -> multiplexer routes to ContainerHost
- `ServiceBusAdministrationClient` sends HTTPS to port 5672 -> multiplexer routes to Kestrel

---

## TCP Multiplexer Component

New class: `AlmostServiceBus.Core.Hosting.TcpMultiplexer`

### Responsibilities

1. Bind a `TcpListener` to the public port
2. Accept TCP connections in a loop
3. Peek the first byte of each connection (without consuming it)
4. Route based on the byte value:
   - `0x41` ('A' — start of AMQP protocol header `AMQP\0\1\0\0`) -> proxy to ContainerHost internal port
   - `0x16` (TLS ClientHello) -> proxy to Kestrel internal HTTPS port
   - Anything else -> close the connection
5. Proxy data bidirectionally between client and backend

### Proxy Implementation

For each routed connection:
1. Open a TCP connection to the target internal port
2. Write the peeked byte to the backend stream
3. Run two concurrent `CopyToAsync` loops: client->backend and backend->client
4. When either direction completes or faults, close both sides

### Lifecycle

- Starts after both ContainerHost and Kestrel are listening
- Stops before either internal server shuts down
- Cancellation via `CancellationToken` from the host lifetime

### Error Handling

- Failed connections: log warning, close both sockets
- Backend unavailable: log error, close client socket
- No retry logic (dev tool, not production infrastructure)

---

## Kestrel HTTPS Configuration

Kestrel switches from plain HTTP to HTTPS using the ASP.NET dev cert:

```csharp
builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpsPort, o => o.UseHttps());
});
```

`UseHttps()` with no arguments uses the dev cert from the machine certificate store automatically.

### Missing Dev Cert Handling

At startup, if Kestrel fails to find the dev cert, catch the exception and print:

```
Error: ASP.NET HTTPS development certificate not found.
Run 'dotnet dev-certs https --trust' to generate and trust the certificate.
```

---

## Changes to Host (Program.cs)

### Port Configuration

Three ports:
- **Public port** (default 5672): exposed to clients, handled by TcpMultiplexer
- **Internal AMQP port**: localhost-only, ContainerHost listens here
- **Internal HTTPS port**: localhost-only, Kestrel listens here

Internal ports are allocated dynamically (bind to port 0, let the OS assign). They are not user-configurable — they are implementation details hidden behind the multiplexer.

### Startup Order

1. Start Kestrel (internal HTTPS port)
2. Start ContainerHost (internal AMQP port)
3. Start TcpMultiplexer (public port, routes to the above)

### Shutdown Order

1. Stop TcpMultiplexer (stop accepting new connections)
2. Stop ContainerHost
3. Stop Kestrel

### Console Output

```
Azure Service Bus Emulator started
  Listening: localhost:5672

  Connection String: Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator
```

---

## Changes to TestHost (ServiceBusEmulatorFixture)

### Port Allocation

Allocate 3 free ports: public, internal AMQP, internal HTTPS.

### Public API Changes

- `ConnectionString` uses the public multiplexer port (works for both SDK clients)
- `AmqpPort` and `HttpPort` become `internal` (implementation details)
- New: `PublicPort` property (the multiplexer port)
- Remove need for `LocalRedirectTransport` in consuming tests

### Startup

Same order as Host: Kestrel -> ContainerHost -> TcpMultiplexer.

---

## Test Updates

### Simplified Tests

- MassTransit tests: remove `LocalRedirectTransport` usage, use plain connection string with `ServiceBusAdministrationClient`
- SDK integration tests: switch `AdminClientTests` from raw `HttpClient` to `ServiceBusAdministrationClient` with connection string

### New Tests

1. **Multiplexer routing test**: verify AMQP and HTTPS both work on the single port simultaneously
2. **Dev cert missing test**: verify helpful error message when dev cert is not available

### Retained

- `LocalRedirectTransport` stays in the codebase but is no longer used by default — can be removed in a follow-up

---

## Migration & Backwards Compatibility

- `HttpPort` / `AmqpPort` on the fixture become `internal`
- The plain HTTP endpoint on Kestrel can optionally be kept as a secondary listener for diagnostics
- `LocalRedirectTransport` is not deleted, just unused — no breaking change for anyone who imported it

---

## Scope

### In Scope

- `TcpMultiplexer` class with first-byte protocol sniffing
- Bidirectional TCP proxy
- Kestrel HTTPS with dev cert
- Host and TestHost wiring updates
- Test simplification
- Dev cert error messaging

### Out of Scope

- AMQP-over-TLS (plain AMQP only)
- Custom certificate configuration (dev cert only for now)
- WebSocket AMQP transport
- Connection pooling or keep-alive optimization in the proxy
