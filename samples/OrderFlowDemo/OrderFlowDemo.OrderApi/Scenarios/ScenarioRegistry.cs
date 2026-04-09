namespace OrderFlowDemo.OrderApi.Scenarios;

public static class ScenarioRegistry
{
    public static readonly Dictionary<string, ScenarioDefinition> All = new(StringComparer.OrdinalIgnoreCase)
    {
        ["steady-state"] = new ScenarioDefinition
        {
            Name = "steady-state",
            Description = "Normal order flow: 2-5 orders/sec, ~5% payment failures",
            OrdersPerSecond = 3,
            PaymentFailureProbability = 0.05,
            InventoryFailureProbability = 0.01,
            ShippingFailureProbability = 0.0,
        },
        ["black-friday"] = new ScenarioDefinition
        {
            Name = "black-friday",
            Description = "Spike traffic: ramps 1→50/sec, high inventory contention",
            OrdersPerSecond = 1,
            RampToOrdersPerSecond = 50,
            RampDuration = TimeSpan.FromSeconds(30),
            HoldDuration = TimeSpan.FromSeconds(60),
            PaymentFailureProbability = 0.03,
            InventoryFailureProbability = 0.15,
            ShippingFailureProbability = 0.02,
        },
        ["bottleneck"] = new ScenarioDefinition
        {
            Name = "bottleneck",
            Description = "All orders to one warehouse — FIFO session queue demo",
            OrdersPerSecond = 5,
            ForcedWarehouseId = "London-East",
            PaymentFailureProbability = 0.0,
            InventoryFailureProbability = 0.0,
            ShippingFailureProbability = 0.0,
        },
        ["failure-cascade"] = new ScenarioDefinition
        {
            Name = "failure-cascade",
            Description = "30% payment failures, 20% shipping failures — DLQ demo",
            OrdersPerSecond = 5,
            PaymentFailureProbability = 0.30,
            InventoryFailureProbability = 0.05,
            ShippingFailureProbability = 0.20,
        },
        ["happy-path"] = new ScenarioDefinition
        {
            Name = "happy-path",
            Description = "1 order/sec, no failures — clean lifecycle walkthrough",
            OrdersPerSecond = 1,
            PaymentFailureProbability = 0.0,
            InventoryFailureProbability = 0.0,
            ShippingFailureProbability = 0.0,
        },
    };
}
