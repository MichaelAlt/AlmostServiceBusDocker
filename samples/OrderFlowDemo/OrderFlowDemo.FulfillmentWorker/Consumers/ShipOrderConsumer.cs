using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.FulfillmentWorker.Consumers;

public class ShipOrderConsumer(ILogger<ShipOrderConsumer> logger) : IConsumer<ShipOrder>
{
    private static int _trackingCounter;

    public async Task Consume(ConsumeContext<ShipOrder> context)
    {
        var msg = context.Message;
        logger.LogInformation("Shipping Order {OrderId} from {Warehouse}", msg.OrderId, msg.WarehouseId);

        await Task.Delay(Random.Shared.Next(150, 600), context.CancellationToken);

        if (Random.Shared.NextDouble() < msg.FailureProbability)
        {
            logger.LogWarning("Shipping failed for Order {OrderId}, will retry", msg.OrderId);
            throw new InvalidOperationException($"Carrier rejected shipment for order {msg.OrderId}");
        }

        var tracking = $"UK-{Interlocked.Increment(ref _trackingCounter):D8}";
        await context.Publish(new OrderShipped
        {
            OrderId = msg.OrderId,
            WarehouseId = msg.WarehouseId,
            TrackingReference = tracking,
        });
    }
}
