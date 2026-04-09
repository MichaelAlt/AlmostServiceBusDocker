using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.OrderApi.Consumers;

public class ProcessPaymentConsumer(ILogger<ProcessPaymentConsumer> logger) : IConsumer<ProcessPayment>
{
    private static readonly Random Rng = Random.Shared;

    public async Task Consume(ConsumeContext<ProcessPayment> context)
    {
        var msg = context.Message;
        logger.LogInformation("Processing payment for Order {OrderId}, Amount: £{Amount:F2}",
            msg.OrderId, msg.Amount);

        // Simulate payment gateway latency
        await Task.Delay(Rng.Next(200, 800), context.CancellationToken);

        if (Rng.NextDouble() < msg.FailureProbability)
        {
            logger.LogWarning("Payment FAILED for Order {OrderId}", msg.OrderId);
            await context.Publish(new PaymentFailed
            {
                OrderId = msg.OrderId,
                Reason = "Card declined by issuer",
            });
            return;
        }

        logger.LogInformation("Payment completed for Order {OrderId}", msg.OrderId);
        await context.Publish(new PaymentCompleted { OrderId = msg.OrderId });
    }
}
