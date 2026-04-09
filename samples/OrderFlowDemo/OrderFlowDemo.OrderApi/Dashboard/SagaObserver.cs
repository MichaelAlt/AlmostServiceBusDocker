using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.OrderApi.Dashboard;

/// <summary>
/// Consume observer that watches saga-relevant events flowing through the bus
/// and forwards state-transition summaries to the dashboard event bus.
/// </summary>
public class SagaObserver(DashboardEventBus eventBus, DashboardStats stats) : IConsumeObserver
{
    public Task PreConsume<T>(ConsumeContext<T> context) where T : class
        => Task.CompletedTask;

    public Task PostConsume<T>(ConsumeContext<T> context) where T : class
    {
        switch (context.Message)
        {
            case OrderSubmitted msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    ToState = "PaymentPending",
                    Warehouse = msg.WarehouseId,
                    CustomerName = msg.CustomerName,
                    Products = msg.Products,
                    Amount = msg.Amount,
                });
                break;

            case PaymentCompleted msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "PaymentPending",
                    ToState = "InventoryReserving",
                });
                break;

            case PaymentFailed msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "PaymentPending",
                    ToState = "PaymentFailed",
                    FailureReason = msg.Reason,
                });
                break;

            case InventoryReserved msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "InventoryReserving",
                    ToState = "Picking",
                    Warehouse = msg.WarehouseId,
                });
                break;

            case InventoryUnavailable msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "InventoryReserving",
                    ToState = "BackOrdered",
                    FailureReason = msg.Reason,
                });
                break;

            case OrderPicked msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "Picking",
                    ToState = "Shipping",
                    Warehouse = msg.WarehouseId,
                });
                break;

            case OrderShipped msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "Shipping",
                    ToState = "Shipped",
                    Warehouse = msg.WarehouseId,
                });
                break;

            case OrderDelivered msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "Shipped",
                    ToState = "Delivered",
                    Warehouse = msg.WarehouseId,
                });
                break;

            case InvoiceGenerated msg:
                Emit(new DashboardEvent
                {
                    Type = "saga-transition",
                    OrderId = msg.OrderId,
                    FromState = "Delivered",
                    ToState = "Invoiced",
                });
                break;
        }

        return Task.CompletedTask;
    }

    public Task ConsumeFault<T>(ConsumeContext<T> context, Exception exception) where T : class
        => Task.CompletedTask;

    private void Emit(DashboardEvent evt)
    {
        eventBus.Publish(evt);
        stats.ProcessEvent(evt);
    }
}
