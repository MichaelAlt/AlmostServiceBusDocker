using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.FulfillmentWorker.Consumers;

public class ReserveInventoryConsumer(ILogger<ReserveInventoryConsumer> logger) : IConsumer<ReserveInventory>
{
    public async Task Consume(ConsumeContext<ReserveInventory> context)
    {
        var msg = context.Message;
        logger.LogInformation("Reserving inventory for Order {OrderId} at {Warehouse}",
            msg.OrderId, msg.WarehouseId);

        await Task.Delay(Random.Shared.Next(100, 400), context.CancellationToken);

        if (Random.Shared.NextDouble() < msg.FailureProbability)
        {
            logger.LogWarning("Inventory unavailable for Order {OrderId}", msg.OrderId);
            await context.Publish(new InventoryUnavailable
            {
                OrderId = msg.OrderId,
                Reason = "Insufficient stock at " + msg.WarehouseId,
            });
            return;
        }

        await context.Publish(new InventoryReserved
        {
            OrderId = msg.OrderId,
            WarehouseId = msg.WarehouseId,
        });
    }
}
