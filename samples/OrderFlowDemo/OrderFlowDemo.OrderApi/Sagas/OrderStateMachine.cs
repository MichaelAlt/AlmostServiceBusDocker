using MassTransit;
using MassTransit.AzureServiceBusTransport;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.OrderApi.Sagas;

public class OrderStateMachine : MassTransitStateMachine<OrderState>
{
    public State Submitted { get; private set; } = null!;
    public State PaymentPending { get; private set; } = null!;
    public State PaymentCompleted { get; private set; } = null!;
    public State InventoryReserving { get; private set; } = null!;
    public State InventoryReserved { get; private set; } = null!;
    public State Picking { get; private set; } = null!;
    public State Picked { get; private set; } = null!;
    public State Shipping { get; private set; } = null!;
    public State Shipped { get; private set; } = null!;
    public State Delivered { get; private set; } = null!;
    public State Invoiced { get; private set; } = null!;
    public State PaymentFailed { get; private set; } = null!;
    public State BackOrdered { get; private set; } = null!;

    public Event<OrderSubmitted> OrderSubmittedEvent { get; private set; } = null!;
    public Event<Contracts.PaymentCompleted> PaymentCompletedEvent { get; private set; } = null!;
    public Event<Contracts.PaymentFailed> PaymentFailedEvent { get; private set; } = null!;
    public Event<Contracts.InventoryReserved> InventoryReservedEvent { get; private set; } = null!;
    public Event<Contracts.InventoryUnavailable> InventoryUnavailableEvent { get; private set; } = null!;
    public Event<Contracts.OrderPicked> OrderPickedEvent { get; private set; } = null!;
    public Event<Contracts.OrderShipped> OrderShippedEvent { get; private set; } = null!;
    public Event<Contracts.OrderDelivered> OrderDeliveredEvent { get; private set; } = null!;
    public Event<Contracts.InvoiceGenerated> InvoiceGeneratedEvent { get; private set; } = null!;

    public OrderStateMachine()
    {
        InstanceState(x => x.CurrentState);

        Event(() => OrderSubmittedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => PaymentCompletedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => PaymentFailedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => InventoryReservedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => InventoryUnavailableEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => OrderPickedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => OrderShippedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => OrderDeliveredEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));
        Event(() => InvoiceGeneratedEvent, x => x.CorrelateById(ctx => ctx.Message.OrderId));

        Initially(
            When(OrderSubmittedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.CustomerName = ctx.Message.CustomerName;
                    ctx.Saga.Products = ctx.Message.Products;
                    ctx.Saga.Amount = ctx.Message.Amount;
                    ctx.Saga.WarehouseId = ctx.Message.WarehouseId;
                    ctx.Saga.PaymentFailureProbability = ctx.Message.PaymentFailureProbability;
                    ctx.Saga.InventoryFailureProbability = ctx.Message.InventoryFailureProbability;
                    ctx.Saga.ShippingFailureProbability = ctx.Message.ShippingFailureProbability;
                    ctx.Saga.CreatedAt = DateTimeOffset.UtcNow;
                    ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow;
                })
                .Send(ctx => new Uri("queue:ProcessPayment"), ctx => new ProcessPayment
                {
                    OrderId = ctx.Saga.CorrelationId,
                    Amount = ctx.Saga.Amount,
                    FailureProbability = ctx.Saga.PaymentFailureProbability,
                })
                .TransitionTo(PaymentPending));

        During(PaymentPending,
            When(PaymentCompletedEvent)
                .Then(ctx => ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow)
                .Send(ctx => new Uri("queue:ReserveInventory"), ctx => new ReserveInventory
                {
                    OrderId = ctx.Saga.CorrelationId,
                    WarehouseId = ctx.Saga.WarehouseId,
                    FailureProbability = ctx.Saga.InventoryFailureProbability,
                })
                .TransitionTo(InventoryReserving));

        During(PaymentPending,
            When(PaymentFailedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.FailureReason = ctx.Message.Reason;
                    ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow;
                })
                .TransitionTo(PaymentFailed));

        During(InventoryReserving,
            When(InventoryReservedEvent)
                .Then(ctx => ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow)
                .Send(ctx => new Uri("queue:PickOrder"), ctx => new PickOrder
                {
                    OrderId = ctx.Saga.CorrelationId,
                    WarehouseId = ctx.Saga.WarehouseId,
                })
                .TransitionTo(Picking));

        During(InventoryReserving,
            When(InventoryUnavailableEvent)
                .Then(ctx =>
                {
                    ctx.Saga.FailureReason = ctx.Message.Reason;
                    ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow;
                })
                .TransitionTo(BackOrdered));

        During(Picking,
            When(OrderPickedEvent)
                .ThenAsync(async ctx =>
                {
                    ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow;
                    var endpoint = await ctx.GetSendEndpoint(new Uri("queue:logistics-dispatch"));
                    await endpoint.Send(new ShipOrder
                    {
                        OrderId = ctx.Saga.CorrelationId,
                        WarehouseId = ctx.Saga.WarehouseId,
                        FailureProbability = ctx.Saga.ShippingFailureProbability,
                    }, sendCtx => sendCtx.SetSessionId(ctx.Saga.WarehouseId));
                })
                .TransitionTo(Shipping));

        During(Shipping,
            When(OrderShippedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.TrackingReference = ctx.Message.TrackingReference;
                    ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow;
                })
                .TransitionTo(Shipped));

        During(Shipped,
            When(OrderDeliveredEvent)
                .Then(ctx => ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow)
                .Send(ctx => new Uri("queue:GenerateInvoice"), ctx => new GenerateInvoice
                {
                    OrderId = ctx.Saga.CorrelationId,
                    Amount = ctx.Saga.Amount,
                })
                .TransitionTo(Delivered));

        During(Delivered,
            When(InvoiceGeneratedEvent)
                .Then(ctx => ctx.Saga.UpdatedAt = DateTimeOffset.UtcNow)
                .TransitionTo(Invoiced)
                .Finalize());

        SetCompletedWhenFinalized();
    }
}
