using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.FulfillmentWorker.Consumers;

public class OrderShippedConsumer(ILogger<OrderShippedConsumer> logger) : IConsumer<OrderShipped>
{
    public async Task Consume(ConsumeContext<OrderShipped> context)
    {
        var msg = context.Message;
        logger.LogInformation("Confirming delivery for Order {OrderId}, tracking {Tracking}",
            msg.OrderId, msg.TrackingReference);

        await Task.Delay(Random.Shared.Next(100, 300), context.CancellationToken);

        await context.Publish(new OrderDelivered
        {
            OrderId = msg.OrderId,
            WarehouseId = msg.WarehouseId,
        });
    }
}
