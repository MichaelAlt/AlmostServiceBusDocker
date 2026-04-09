namespace OrderFlowDemo.Contracts;

public record ProcessPayment
{
    public Guid OrderId { get; init; }
    public decimal Amount { get; init; }
    public double FailureProbability { get; init; }
}

public record ReserveInventory
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
    public double FailureProbability { get; init; }
}

public record PickOrder
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
}

public record ShipOrder
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
    public double FailureProbability { get; init; }
}

public record GenerateInvoice
{
    public Guid OrderId { get; init; }
    public decimal Amount { get; init; }
}
