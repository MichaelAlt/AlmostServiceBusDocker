using MassTransit;

namespace OrderFlowDemo.OrderApi.Sagas;

public class OrderState : SagaStateMachineInstance
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string[] Products { get; set; } = [];
    public decimal Amount { get; set; }
    public string WarehouseId { get; set; } = "";
    public string? TrackingReference { get; set; }
    public string? FailureReason { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }

    // Failure probabilities passed from scenario
    public double PaymentFailureProbability { get; set; }
    public double InventoryFailureProbability { get; set; }
    public double ShippingFailureProbability { get; set; }
}
