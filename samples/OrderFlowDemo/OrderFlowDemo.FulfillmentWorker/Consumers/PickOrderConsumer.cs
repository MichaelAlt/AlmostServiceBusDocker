using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.FulfillmentWorker.Consumers;

public class PickOrderConsumer(ILogger<PickOrderConsumer> logger) : IConsumer<PickOrder>
{
    public async Task Consume(ConsumeContext<PickOrder> context)
    {
        var msg = context.Message;
        logger.LogInformation("Picking order {OrderId} at {Warehouse}", msg.OrderId, msg.WarehouseId);

        await Task.Delay(Random.Shared.Next(150, 500), context.CancellationToken);

        await context.Publish(new OrderPicked
        {
            OrderId = msg.OrderId,
            WarehouseId = msg.WarehouseId,
        });
    }
}
