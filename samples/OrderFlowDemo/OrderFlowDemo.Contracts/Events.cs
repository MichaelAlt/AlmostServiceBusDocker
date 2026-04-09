namespace OrderFlowDemo.Contracts;

public record OrderSubmitted
{
    public Guid OrderId { get; init; }
    public string CustomerName { get; init; } = "";
    public string[] Products { get; init; } = [];
    public decimal Amount { get; init; }
    public string WarehouseId { get; init; } = "";
    public double PaymentFailureProbability { get; init; }
    public double InventoryFailureProbability { get; init; }
    public double ShippingFailureProbability { get; init; }
}

public record PaymentCompleted
{
    public Guid OrderId { get; init; }
}

public record PaymentFailed
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = "";
}

public record InventoryReserved
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
}

public record InventoryUnavailable
{
    public Guid OrderId { get; init; }
    public string Reason { get; init; } = "";
}

public record OrderPicked
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
}

public record OrderShipped
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
    public string TrackingReference { get; init; } = "";
}

public record OrderDelivered
{
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = "";
}

public record InvoiceGenerated
{
    public Guid OrderId { get; init; }
}
