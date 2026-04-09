using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.OrderApi.Scenarios;

public class ScenarioEngine(IBus bus, ILogger<ScenarioEngine> logger) : BackgroundService
{
    private volatile ScenarioDefinition? _active;
    private readonly Lock _lock = new();
    private DateTimeOffset _startedAt;
    private int _orderCount;

    public ScenarioDefinition? ActiveScenario => _active;
    public DateTimeOffset StartedAt => _startedAt;
    public int OrderCount => _orderCount;

    public void Start(ScenarioDefinition scenario)
    {
        lock (_lock)
        {
            _active = scenario;
            _startedAt = DateTimeOffset.UtcNow;
            Interlocked.Exchange(ref _orderCount, 0);
        }
        logger.LogInformation("Scenario started: {Name}", scenario.Name);
    }

    public void Stop()
    {
        lock (_lock)
        {
            if (_active != null)
            {
                logger.LogInformation("Scenario stopped: {Name}", _active.Name);
                _active = null;
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var scenario = _active;
            if (scenario is null)
            {
                await Task.Delay(100, stoppingToken);
                continue;
            }

            var rate = CalculateCurrentRate(scenario);
            if (rate <= 0)
            {
                _active = null; // Scenario complete (ramp finished)
                continue;
            }

            var delayMs = (int)(1000.0 / rate);

            var orderId = Guid.NewGuid();
            var (products, amount) = OrderDataGenerator.RandomProducts();
            var warehouseId = scenario.ForcedWarehouseId ?? OrderDataGenerator.RandomWarehouse();

            await bus.Publish(new OrderSubmitted
            {
                OrderId = orderId,
                CustomerName = OrderDataGenerator.RandomCustomer(),
                Products = products,
                Amount = amount,
                WarehouseId = warehouseId,
                PaymentFailureProbability = scenario.PaymentFailureProbability,
                InventoryFailureProbability = scenario.InventoryFailureProbability,
                ShippingFailureProbability = scenario.ShippingFailureProbability,
            }, stoppingToken);

            Interlocked.Increment(ref _orderCount);
            await Task.Delay(delayMs, stoppingToken);
        }
    }

    private double CalculateCurrentRate(ScenarioDefinition scenario)
    {
        if (scenario.RampToOrdersPerSecond is not { } rampTo)
            return scenario.OrdersPerSecond;

        var elapsed = DateTimeOffset.UtcNow - _startedAt;
        var rampDuration = scenario.RampDuration ?? TimeSpan.FromSeconds(30);
        var holdDuration = scenario.HoldDuration ?? TimeSpan.FromSeconds(60);

        if (elapsed < rampDuration)
        {
            var progress = elapsed / rampDuration;
            return scenario.OrdersPerSecond + (rampTo - scenario.OrdersPerSecond) * progress;
        }

        if (elapsed < rampDuration + holdDuration)
            return rampTo;

        if (elapsed < rampDuration + holdDuration + rampDuration)
        {
            var rampDownProgress = (elapsed - rampDuration - holdDuration) / rampDuration;
            return rampTo - (rampTo - scenario.OrdersPerSecond) * rampDownProgress;
        }

        return -1; // Signal scenario is complete
    }
}
