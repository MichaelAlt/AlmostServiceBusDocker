# TCP Multiplexer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the emulator serve both AMQP and HTTPS on a single port so users can use a plain connection string with no custom transport.

**Architecture:** A TCP multiplexer listens on the public port, peeks the first byte of each connection, and proxies to either ContainerHost (AMQP, byte `0x41`) or Kestrel HTTPS (TLS, byte `0x16`) on internal localhost-only ports. Both backend servers remain unchanged.

**Tech Stack:** .NET 10, ASP.NET Core Kestrel, AMQPNetLite 2.5.1, `System.Net.Sockets.TcpListener`

**Spec:** `docs/superpowers/specs/2026-04-01-tcp-multiplexer-design.md`

---

### Task 1: TcpMultiplexer — Core proxy class

**Files:**
- Create: `src/AlmostServiceBus.Core/Hosting/TcpMultiplexer.cs`
- Test: `tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs`

This is the core component. It listens on a port, peeks the first byte, and proxies the full TCP stream to one of two backend ports.

- [ ] **Step 1: Write the failing test — routes AMQP-like connections**

Create the test file with a test that starts a simple TCP echo server on an internal port, starts the multiplexer, connects with a byte stream starting with `0x41`, and verifies the connection is proxied to the correct backend.

```csharp
// tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs
using System.Net;
using System.Net.Sockets;
using System.Text;
using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Tests.Hosting;

public class TcpMultiplexerTests : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Starts a TCP server that echoes back everything it receives, prefixed with a tag.
    /// </summary>
    private Task StartEchoServer(int port, string tag, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(ct);
                    _ = Task.Run(async () =>
                    {
                        using (client)
                        {
                            var stream = client.GetStream();
                            var buffer = new byte[1024];
                            var read = await stream.ReadAsync(buffer, ct);
                            var received = Encoding.UTF8.GetString(buffer, 0, read);
                            var response = Encoding.UTF8.GetBytes($"{tag}:{received}");
                            await stream.WriteAsync(response, ct);
                            client.Client.Shutdown(SocketShutdown.Send);
                        }
                    }, ct);
                }
            }
            finally
            {
                listener.Stop();
            }
        }, ct);
    }

    [Fact]
    public async Task Routes_AmqpConnection_ToAmqpBackend()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100); // let servers bind

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with 0x41 ('A' — AMQP protocol header)
        var payload = Encoding.UTF8.GetBytes("AMQP-test-data");
        await stream.WriteAsync(payload);
        client.Client.Shutdown(SocketShutdown.Send);

        var buffer = new byte[1024];
        var read = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, read);

        Assert.StartsWith("AMQP:", response);
        Assert.Contains("AMQP-test-data", response);
    }

    [Fact]
    public async Task Routes_TlsConnection_ToHttpsBackend()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with 0x16 (TLS ClientHello)
        var payload = new byte[] { 0x16, 0x03, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05 };
        await stream.WriteAsync(payload);
        client.Client.Shutdown(SocketShutdown.Send);

        var buffer = new byte[1024];
        var read = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, read);

        Assert.StartsWith("HTTPS:", response);
    }

    [Fact]
    public async Task Closes_Connection_OnUnknownProtocol()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpsPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpsPort, "HTTPS", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        // Send data starting with an unknown byte
        var payload = new byte[] { 0xFF, 0x01, 0x02 };
        await stream.WriteAsync(payload);

        // Connection should be closed by multiplexer
        var buffer = new byte[1024];
        var read = await stream.ReadAsync(buffer);
        Assert.Equal(0, read); // connection closed
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~TcpMultiplexerTests" --no-build 2>&1 || true`

Expected: Build failure — `TcpMultiplexer` class does not exist.

- [ ] **Step 3: Implement TcpMultiplexer**

```csharp
// src/AlmostServiceBus.Core/Hosting/TcpMultiplexer.cs
using System.Net;
using System.Net.Sockets;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Listens on a single public port and routes connections to either the AMQP backend
/// or the HTTPS backend based on the first byte of the connection.
///
/// AMQP connections start with 0x41 ('A' from the "AMQP" protocol header).
/// TLS connections start with 0x16 (TLS ClientHello record type).
/// </summary>
public class TcpMultiplexer
{
    private const byte AmqpByte = 0x41; // 'A' — start of "AMQP\0\1\0\0"
    private const byte TlsByte = 0x16;  // TLS record type: Handshake

    private readonly int _listenPort;
    private readonly int _amqpPort;
    private readonly int _httpsPort;

    public TcpMultiplexer(int listenPort, int amqpPort, int httpsPort)
    {
        _listenPort = listenPort;
        _amqpPort = amqpPort;
        _httpsPort = httpsPort;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _listenPort);
        listener.Start();

        ct.Register(() => listener.Stop());

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(ct);
                _ = HandleConnectionAsync(client, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken ct)
    {
        try
        {
            var stream = client.GetStream();

            // Peek the first byte
            var firstByte = new byte[1];
            var read = await stream.ReadAsync(firstByte, 0, 1, ct);
            if (read == 0)
            {
                client.Dispose();
                return;
            }

            var backendPort = firstByte[0] switch
            {
                AmqpByte => _amqpPort,
                TlsByte => _httpsPort,
                _ => -1
            };

            if (backendPort == -1)
            {
                client.Dispose();
                return;
            }

            // Connect to backend
            var backend = new TcpClient();
            await backend.ConnectAsync(IPAddress.Loopback, backendPort, ct);
            var backendStream = backend.GetStream();

            // Send the peeked byte first
            await backendStream.WriteAsync(firstByte, 0, 1, ct);

            // Bidirectional proxy
            var clientToBackend = stream.CopyToAsync(backendStream, ct)
                .ContinueWith(_ => backend.Client.Shutdown(SocketShutdown.Send), TaskContinuationOptions.OnlyOnRanToCompletion);
            var backendToClient = backendStream.CopyToAsync(stream, ct)
                .ContinueWith(_ => client.Client.Shutdown(SocketShutdown.Send), TaskContinuationOptions.OnlyOnRanToCompletion);

            await Task.WhenAll(clientToBackend, backendToClient);
        }
        catch
        {
            // Connection error — just clean up
        }
        finally
        {
            client.Dispose();
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~TcpMultiplexerTests" -v normal`

Expected: All 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/AlmostServiceBus.Core/Hosting/TcpMultiplexer.cs tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs
git commit -m "feat: add TcpMultiplexer for single-port AMQP/HTTPS routing"
```

---

### Task 2: Configure Kestrel with HTTPS using ASP.NET dev cert

**Files:**
- Modify: `src/AlmostServiceBus.TestHost/ServiceBusEmulatorFixture.cs`
- Test: `tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs` (add integration test)

Switch Kestrel from plain HTTP to HTTPS using the ASP.NET dev cert, and wire up the multiplexer in the test fixture.

- [ ] **Step 1: Write the failing integration test**

Add a test to the existing `TcpMultiplexerTests` that verifies a real HTTPS request gets proxied through the multiplexer to a Kestrel HTTPS backend.

```csharp
// Add to tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs

[Fact]
public async Task Routes_RealHttps_ToKestrelBackend()
{
    var publicPort = GetFreePort();
    var amqpPort = GetFreePort();
    var httpsPort = GetFreePort();

    // Start a real Kestrel HTTPS server on the internal port
    var builder = WebApplication.CreateBuilder();
    builder.WebHost.ConfigureKestrel(k =>
    {
        k.ListenLocalhost(httpsPort, o => o.UseHttps());
    });
    builder.Logging.ClearProviders();
    var app = builder.Build();
    app.MapGet("/health", () => "ok");
    await app.StartAsync();

    try
    {
        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpsPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        // Make an HTTPS request through the multiplexer
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true
        };
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri($"https://localhost:{publicPort}")
        };

        var response = await httpClient.GetStringAsync("/health");
        Assert.Equal("ok", response);
    }
    finally
    {
        await app.StopAsync();
        await app.DisposeAsync();
    }
}
```

Add these usings at the top of the test file:

```csharp
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~Routes_RealHttps" -v normal`

Expected: PASS (if dev cert is installed) or a clear TLS/cert error if not. This test validates the multiplexer correctly forwards TLS connections.

- [ ] **Step 3: Update ServiceBusEmulatorFixture to use HTTPS + multiplexer**

Replace the fixture's startup to allocate 3 ports, configure Kestrel with HTTPS, and start the multiplexer.

```csharp
// src/AlmostServiceBus.TestHost/ServiceBusEmulatorFixture.cs
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Hosting;
using AlmostServiceBus.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.TestHost;

public class ServiceBusEmulatorFixture : IAsyncDisposable
{
    private WebApplication? _webApp;
    private AmqpServer? _amqpServer;
    private TcpMultiplexer? _multiplexer;
    private CancellationTokenSource? _multiplexerCts;
    private readonly NamespaceRegistry _registry = new();
    private readonly string _namespace;

    public int PublicPort { get; private set; }
    internal int AmqpPort { get; private set; }
    internal int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://{_namespace}.localhost:{PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture()
    {
        _namespace = $"test-{Guid.NewGuid():N}"[..20];
    }

    public async Task StartAsync()
    {
        PublicPort = GetFreePort();
        AmqpPort = GetFreePort();
        HttpPort = GetFreePort();

        // 1. Start Kestrel with HTTPS on internal port
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(HttpPort, o => o.UseHttps());
        });
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);
        await _webApp.StartAsync();

        // 2. Start AMQP on internal port
        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry);
        _amqpServer.Start();

        // 3. Start multiplexer on public port
        _multiplexerCts = new CancellationTokenSource();
        _multiplexer = new TcpMultiplexer(PublicPort, AmqpPort, HttpPort);
        _ = _multiplexer.StartAsync(_multiplexerCts.Token);
    }

    public async Task StopAsync()
    {
        if (_multiplexerCts is not null)
            await _multiplexerCts.CancelAsync();
        _amqpServer?.Stop();
        if (_webApp is not null)
            await _webApp.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _amqpServer?.Dispose();
        _multiplexerCts?.Dispose();
        if (_webApp is not null)
            await _webApp.DisposeAsync();
    }

    public NamespaceContext GetNamespaceContext() => _registry.GetOrCreate(_namespace);

    public NamespaceContext GetDefaultNamespaceContext() => _registry.GetOrCreate("default");

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

- [ ] **Step 4: Run existing tests to check nothing broke**

Run: `dotnet test tests/AlmostServiceBus.Tests -v normal`

Expected: All tests pass. The unit tests don't go through the fixture, so they should be unaffected.

- [ ] **Step 5: Commit**

```bash
git add src/AlmostServiceBus.TestHost/ServiceBusEmulatorFixture.cs tests/AlmostServiceBus.Tests/Hosting/TcpMultiplexerTests.cs
git commit -m "feat: wire up HTTPS Kestrel and TcpMultiplexer in test fixture"
```

---

### Task 3: Update MassTransit tests to use plain connection string

**Files:**
- Modify: `tests/AlmostServiceBus.MassTransit.Tests/MassTransitTopologyTests.cs`
- Modify: `tests/AlmostServiceBus.MassTransit.Tests/MassTransitPubSubTests.cs`

Remove `LocalRedirectTransport` usage. The admin client should now work with just the connection string, since HTTPS goes through the multiplexer to Kestrel.

- [ ] **Step 1: Update MassTransitTopologyTests**

Replace the `InitializeAsync` method to use a plain `ServiceBusAdministrationClient` with no custom transport. Add a handler to bypass dev cert validation.

```csharp
// tests/AlmostServiceBus.MassTransit.Tests/MassTransitTopologyTests.cs
// Replace the InitializeAsync method (lines 27-39):

public async Task InitializeAsync()
{
    await _fixture.StartAsync();

    var options = new ServiceBusAdministrationClientOptions();
    options.Transport = new HttpClientTransport(
        new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true
        }));

    _adminClient = new ServiceBusAdministrationClient(
        _fixture.ConnectionString,
        options);
}
```

Add this using at the top of the file:

```csharp
using Azure.Core.Pipeline;
```

- [ ] **Step 2: Update MassTransitPubSubTests**

Same change — replace `LocalRedirectTransport` with the cert-bypassing transport.

```csharp
// tests/AlmostServiceBus.MassTransit.Tests/MassTransitPubSubTests.cs
// Replace the InitializeAsync method (lines 32-39):

public async Task InitializeAsync()
{
    await _fixture.StartAsync();

    var options = new ServiceBusAdministrationClientOptions();
    options.Transport = new HttpClientTransport(
        new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true
        }));

    _adminClient = new ServiceBusAdministrationClient(
        _fixture.ConnectionString,
        options);
}
```

Add this using at the top of the file:

```csharp
using Azure.Core.Pipeline;
```

Update the `OpenAmqpConnectionAsync` method to use the public port through the multiplexer:

```csharp
// tests/AlmostServiceBus.MassTransit.Tests/MassTransitPubSubTests.cs
// Replace the OpenAmqpConnectionAsync method (lines 47-51):

private async Task<Connection> OpenAmqpConnectionAsync()
{
    var address = new Address("localhost", _fixture.PublicPort, null, null, "/", "AMQP");
    return await Connection.Factory.CreateAsync(address);
}
```

- [ ] **Step 3: Run MassTransit topology tests**

Run: `dotnet test tests/AlmostServiceBus.MassTransit.Tests --filter "FullyQualifiedName~TopologyTests" -v normal`

Expected: All topology tests pass — admin client creates entities over HTTPS through the multiplexer.

- [ ] **Step 4: Run MassTransit pub/sub tests**

Run: `dotnet test tests/AlmostServiceBus.MassTransit.Tests --filter "FullyQualifiedName~PubSubTests" -v normal`

Expected: All pub/sub tests pass — AMQP messaging goes through the multiplexer, admin calls go through HTTPS.

- [ ] **Step 5: Commit**

```bash
git add tests/AlmostServiceBus.MassTransit.Tests/MassTransitTopologyTests.cs tests/AlmostServiceBus.MassTransit.Tests/MassTransitPubSubTests.cs
git commit -m "refactor: remove LocalRedirectTransport from MassTransit tests, use plain connection string"
```

---

### Task 4: Update SDK integration tests

**Files:**
- Modify: `tests/AlmostServiceBus.SdkIntegration.Tests/AdminClientTests.cs`
- Modify: `tests/AlmostServiceBus.SdkIntegration.Tests/MessagingTests.cs`

Switch `AdminClientTests` from raw `HttpClient` to `ServiceBusAdministrationClient` with a plain connection string. Update `MessagingTests` to use the public port.

- [ ] **Step 1: Rewrite AdminClientTests to use ServiceBusAdministrationClient**

Replace the raw HTTP approach with the real Azure SDK admin client, proving the emulator is now transparent.

```csharp
// tests/AlmostServiceBus.SdkIntegration.Tests/AdminClientTests.cs
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class AdminClientTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private ServiceBusAdministrationClient _adminClient = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var options = new ServiceBusAdministrationClientOptions();
        options.Transport = new HttpClientTransport(
            new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            }));

        _adminClient = new ServiceBusAdministrationClient(
            _fixture.ConnectionString,
            options);
    }

    public async Task DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task CreateQueue_ThenGetQueue_RoundTrips()
    {
        var created = await _adminClient.CreateQueueAsync("test-queue");
        Assert.Equal("test-queue", created.Value.Name);

        var fetched = await _adminClient.GetQueueAsync("test-queue");
        Assert.Equal("test-queue", fetched.Value.Name);
    }

    [Fact]
    public async Task CreateTopicAndSubscription_Works()
    {
        await _adminClient.CreateTopicAsync("my-topic");

        var subOptions = new CreateSubscriptionOptions("my-topic", "sub-1")
        {
            ForwardTo = "some-queue"
        };
        // Create the forward target first
        await _adminClient.CreateQueueAsync("some-queue");
        var sub = await _adminClient.CreateSubscriptionAsync(subOptions);

        Assert.Equal("sub-1", sub.Value.SubscriptionName);
        Assert.Equal("some-queue", sub.Value.ForwardTo);
    }

    [Fact]
    public async Task GetNonexistentEntity_Throws404()
    {
        var ex = await Assert.ThrowsAsync<Azure.Messaging.ServiceBus.ServiceBusException>(
            () => _adminClient.GetQueueAsync("nonexistent"));

        Assert.Equal(Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task CreateSubscriptionWithRules_Works()
    {
        await _adminClient.CreateTopicAsync("rules-topic");
        await _adminClient.CreateSubscriptionAsync("rules-topic", "rules-sub");

        var ruleOptions = new CreateRuleOptions("my-rule")
        {
            Filter = new SqlRuleFilter("color = 'blue'")
        };
        var rule = await _adminClient.CreateRuleAsync("rules-topic", "rules-sub", ruleOptions);

        Assert.Equal("my-rule", rule.Value.Name);
        Assert.IsType<SqlRuleFilter>(rule.Value.Filter);
    }

    [Fact]
    public async Task DeleteEntity_Works()
    {
        await _adminClient.CreateQueueAsync("delete-me");
        Assert.True((await _adminClient.QueueExistsAsync("delete-me")).Value);

        await _adminClient.DeleteQueueAsync("delete-me");
        Assert.False((await _adminClient.QueueExistsAsync("delete-me")).Value);
    }

    [Fact]
    public async Task UpdateSubscription_WithIfMatch_Works()
    {
        await _adminClient.CreateTopicAsync("update-topic");
        await _adminClient.CreateSubscriptionAsync("update-topic", "update-sub");

        var sub = await _adminClient.GetSubscriptionAsync("update-topic", "update-sub");
        sub.Value.MaxDeliveryCount = 5;
        var updated = await _adminClient.UpdateSubscriptionAsync(sub.Value);

        Assert.Equal(5, updated.Value.MaxDeliveryCount);
    }
}
```

- [ ] **Step 2: Update MessagingTests to use public port**

Change `OpenConnectionAsync` to connect through the multiplexer's public port.

```csharp
// tests/AlmostServiceBus.SdkIntegration.Tests/MessagingTests.cs
// Replace the OpenConnectionAsync method (lines 21-25):

private async Task<Connection> OpenConnectionAsync()
{
    var address = new Address("localhost", _fixture.PublicPort, null, null, "/", "AMQP");
    return await Connection.Factory.CreateAsync(address);
}
```

- [ ] **Step 3: Run SDK integration tests**

Run: `dotnet test tests/AlmostServiceBus.SdkIntegration.Tests -v normal`

Expected: All tests pass — admin operations go through HTTPS via the multiplexer, AMQP messaging goes through the multiplexer.

- [ ] **Step 4: Commit**

```bash
git add tests/AlmostServiceBus.SdkIntegration.Tests/AdminClientTests.cs tests/AlmostServiceBus.SdkIntegration.Tests/MessagingTests.cs
git commit -m "refactor: switch SDK integration tests to use multiplexer, plain connection string"
```

---

### Task 5: Update Host (Program.cs)

**Files:**
- Modify: `src/AlmostServiceBus.Host/Program.cs`

Wire up the multiplexer in the standalone host, matching the test fixture pattern.

- [ ] **Step 1: Update Program.cs**

```csharp
// src/AlmostServiceBus.Host/Program.cs
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Hosting;
using AlmostServiceBus.Core.Management;
using System.Net;
using System.Net.Sockets;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.SetMinimumLevel(LogLevel.Warning);

var publicPort = builder.Configuration.GetValue("Port", 5672);
var internalHttpsPort = GetFreePort();
var internalAmqpPort = GetFreePort();

var registry = new NamespaceRegistry();

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpsPort, o => o.UseHttps());
});

var app = builder.Build();

app.MapServiceBusManagementApi(registry);

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = internalAmqpPort }, registry);
amqpServer.Start();

var multiplexerCts = new CancellationTokenSource();
var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpsPort);

app.Lifetime.ApplicationStopping.Register(() =>
{
    multiplexerCts.Cancel();
    amqpServer.Stop();
});

_ = multiplexer.StartAsync(multiplexerCts.Token);

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  Listening: localhost:{publicPort}");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

try
{
    app.Run();
}
catch (InvalidOperationException ex) when (ex.Message.Contains("certificate"))
{
    Console.Error.WriteLine();
    Console.Error.WriteLine("Error: ASP.NET HTTPS development certificate not found.");
    Console.Error.WriteLine("Run 'dotnet dev-certs https --trust' to generate and trust the certificate.");
    Environment.Exit(1);
}

static int GetFreePort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
```

- [ ] **Step 2: Run the host manually to verify startup**

Run: `dotnet run --project src/AlmostServiceBus.Host -- --Port 5672 2>&1 &` and verify the output shows the expected startup message.

Expected output:
```
Azure Service Bus Emulator started
  Listening: localhost:5672

  Connection String: Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator
```

- [ ] **Step 3: Commit**

```bash
git add src/AlmostServiceBus.Host/Program.cs
git commit -m "feat: wire up TcpMultiplexer in standalone host"
```

---

### Task 6: Delete LocalRedirectTransport and run full test suite

**Files:**
- Delete: `tests/AlmostServiceBus.MassTransit.Tests/LocalRedirectTransport.cs`

- [ ] **Step 1: Delete LocalRedirectTransport**

Delete the file `tests/AlmostServiceBus.MassTransit.Tests/LocalRedirectTransport.cs` — it is no longer referenced by any test.

```bash
git rm tests/AlmostServiceBus.MassTransit.Tests/LocalRedirectTransport.cs
```

- [ ] **Step 2: Verify the solution builds**

Run: `dotnet build`

Expected: Build succeeds with no errors. No remaining references to `LocalRedirectTransport`.

- [ ] **Step 3: Run the full test suite**

Run: `dotnet test --verbosity normal`

Expected: All tests pass across all three test projects:
- `AlmostServiceBus.Tests` — unit tests + multiplexer tests
- `AlmostServiceBus.SdkIntegration.Tests` — SDK admin client + AMQP messaging through multiplexer
- `AlmostServiceBus.MassTransit.Tests` — topology + pub/sub through multiplexer

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: remove LocalRedirectTransport, all tests use multiplexer"
```
