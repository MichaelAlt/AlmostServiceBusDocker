using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Tests that reproduce the Wolverine failure path: ServiceBusProcessor with
/// UseDevelopmentEmulator=true (plain AMQP, no TLS) receiving batch messages.
/// </summary>
public class ProcessorPlainAmqpTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    /// <summary>
    /// Creates a client using the same connection path as Wolverine's UseDevelopmentEmulator=true:
    /// plain AMQP (no TLS) directly to the public port.
    /// </summary>
    private ServiceBusClient CreatePlainAmqpClient()
    {
        // UseDevelopmentEmulator=true in the connection string tells the SDK to:
        // 1. Use plain AMQP (no TLS)
        // 2. Use AmqpTcp transport type
        // 3. Connect directly to the specified port
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        return new ServiceBusClient(cs);
    }

    [Fact]
    public async Task PlainAmqp_SingleMessage_Processor_ReceivesAndCompletes()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("plain-proc-single");

        await using var client = CreatePlainAmqpClient();
        var sender = client.CreateSender("plain-proc-single");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { Subject = "PlainTestType" });
        await sender.CloseAsync();

        var received = new TaskCompletionSource<string>();
        var processor = client.CreateProcessor("plain-proc-single");
        processor.ProcessMessageAsync += args =>
        {
            received.TrySetResult(args.Message.Subject);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[PLAIN-PROC-ERROR] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var subject = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await processor.StopProcessingAsync();

        Assert.Equal("PlainTestType", subject);
    }

    [Fact]
    public async Task PlainAmqp_TwoMessages_SentAsBatch_Processor_ReceivesBoth()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("plain-proc-batch");

        await using var client = CreatePlainAmqpClient();
        var sender = client.CreateSender("plain-proc-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "PlainBatch1", MessageId = "plain-batch-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "PlainBatch2", MessageId = "plain-batch-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var errors = new ConcurrentBag<string>();

        var processor = client.CreateProcessor("plain-proc-batch");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args =>
        {
            errors.Add($"{args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]. Errors: [{string.Join("; ", errors)}]");
    }

    [Fact]
    public async Task PlainAmqp_TwoMessages_SentAsBatch_Receiver_ReceivesBoth()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("plain-recv-batch");

        await using var client = CreatePlainAmqpClient();
        var sender = client.CreateSender("plain-recv-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "PlainRBatch1", MessageId = "plain-rbatch-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "PlainRBatch2", MessageId = "plain-rbatch-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var receiver = client.CreateReceiver("plain-recv-batch");
        var messages = new List<ServiceBusReceivedMessage>();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (messages.Count < 2 && sw.Elapsed < TimeSpan.FromSeconds(15))
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
            if (msg != null)
            {
                messages.Add(msg);
                await receiver.CompleteMessageAsync(msg);
            }
        }
        await receiver.CloseAsync();

        Assert.Equal(2, messages.Count);
    }

    [Fact]
    public async Task PlainAmqp_TwoMessages_SentIndividually_Processor_ReceivesBoth()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("plain-proc-individual");

        await using var client = CreatePlainAmqpClient();
        var sender = client.CreateSender("plain-proc-individual");
        await sender.SendMessageAsync(new ServiceBusMessage("msg1") { Subject = "PlainInd1" });
        await sender.SendMessageAsync(new ServiceBusMessage("msg2") { Subject = "PlainInd2" });
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        var processor = client.CreateProcessor("plain-proc-individual");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }
}
