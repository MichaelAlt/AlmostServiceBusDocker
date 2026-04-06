using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Tests that mimic Wolverine's two-host pattern: separate ServiceBusClient instances
/// for sender and receiver, each with their own AMQP connection.
/// </summary>
public class TwoClientProcessorTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient()
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        return new ServiceBusClient(cs);
    }

    [Fact]
    public async Task TwoClients_BatchSend_ProcessorReceive_BothDelivered()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("two-client-batch");

        // Separate client for sender (simulates Wolverine sender host)
        await using var senderClient = CreateClient();
        var sender = senderClient.CreateSender("two-client-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "TwoClient1", MessageId = "tc-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "TwoClient2", MessageId = "tc-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        // Separate client for receiver (simulates Wolverine receiver host)
        await using var receiverClient = CreateClient();
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        var processor = receiverClient.CreateProcessor("two-client-batch");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[2CLIENT-ERROR] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }

    [Fact]
    public async Task TwoClients_BatchSend_ProcessorWithSlowHandler_BothDelivered()
    {
        // Wolverine's handler pipeline takes some time — simulate with a delay
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("two-client-slow");

        await using var senderClient = CreateClient();
        var sender = senderClient.CreateSender("two-client-slow");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "Slow1", MessageId = "slow-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "Slow2", MessageId = "slow-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        await using var receiverClient = CreateClient();
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        var processor = receiverClient.CreateProcessor("two-client-slow");
        processor.ProcessMessageAsync += async args =>
        {
            // Simulate Wolverine handler pipeline processing time
            await Task.Delay(50);
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }

    [Fact]
    public async Task TwoClients_BatchSend_ProcessorExplicitComplete_BothDelivered()
    {
        // Wolverine calls CompleteAsync during processing AND processor auto-completes
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("two-client-explicit");

        await using var senderClient = CreateClient();
        var sender = senderClient.CreateSender("two-client-explicit");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "Ex1", MessageId = "ex-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "Ex2", MessageId = "ex-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        await using var receiverClient = CreateClient();
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        // AutoCompleteMessages=true (default) + explicit complete = double complete
        var processor = receiverClient.CreateProcessor("two-client-explicit");
        processor.ProcessMessageAsync += async args =>
        {
            // Explicitly complete like Wolverine does
            await args.CompleteMessageAsync(args.Message);
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[2CLIENT-EXPLICIT-ERROR] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }

    [Fact]
    public async Task TwoClients_BatchSend_ProcessorSendsResponseDuringHandler()
    {
        // This mimics the tracking test: handler processes msg AND sends new messages
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("two-client-cascade-in");
        context.CreateQueue("two-client-cascade-out");

        await using var senderClient = CreateClient();
        var sender = senderClient.CreateSender("two-client-cascade-in");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "Cascade1", MessageId = "casc-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "Cascade2", MessageId = "casc-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        await using var receiverClient = CreateClient();
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();

        // Handler sends a response message during processing (like Wolverine cascading)
        var responseSender = receiverClient.CreateSender("two-client-cascade-out");

        var processor = receiverClient.CreateProcessor("two-client-cascade-in");
        processor.ProcessMessageAsync += async args =>
        {
            // Simulate Wolverine handler that sends a response
            await responseSender.SendMessageAsync(
                new ServiceBusMessage($"response-to-{args.Message.Subject}") { Subject = "Response" });

            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[CASCADE-ERROR] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();
        await responseSender.CloseAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }
}
