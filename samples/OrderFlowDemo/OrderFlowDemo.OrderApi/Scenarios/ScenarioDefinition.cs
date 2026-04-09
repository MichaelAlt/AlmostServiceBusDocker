namespace OrderFlowDemo.OrderApi.Scenarios;

public record ScenarioDefinition
{
    public required string Name { get; init; }
    public required string Description { get; init; }
    public required double OrdersPerSecond { get; init; }
    public double? RampToOrdersPerSecond { get; init; }
    public TimeSpan? RampDuration { get; init; }
    public TimeSpan? HoldDuration { get; init; }
    public double PaymentFailureProbability { get; init; }
    public double InventoryFailureProbability { get; init; }
    public double ShippingFailureProbability { get; init; }
    public string? ForcedWarehouseId { get; init; }
}
