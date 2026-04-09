using System.Collections.Concurrent;

namespace OrderFlowDemo.OrderApi.Dashboard;

public class DashboardStats
{
    private static readonly string[] TerminalStates = ["Invoiced", "Final"];
    private static readonly string[] FailureStates = ["PaymentFailed", "BackOrdered"];

    private readonly ConcurrentDictionary<string, int> _pipelineCounts = new();
    private readonly ConcurrentDictionary<string, int> _warehouseDepths = new();

    public int Total;
    public int Completed;
    public int Failed;

    public void ProcessEvent(DashboardEvent evt)
    {
        if (evt.Type != "saga-transition") return;

        var toState = evt.ToState;
        if (toState is null) return;

        // Decrement old state
        if (evt.FromState is not null)
            _pipelineCounts.AddOrUpdate(evt.FromState, 0, (_, v) => Math.Max(0, v - 1));

        // Increment new state
        _pipelineCounts.AddOrUpdate(toState, 1, (_, v) => v + 1);

        // Track totals
        if (evt.FromState is null)
        {
            Interlocked.Increment(ref Total);
            if (evt.Warehouse is not null)
                _warehouseDepths.AddOrUpdate(evt.Warehouse, 1, (_, v) => v + 1);
        }

        if (TerminalStates.Contains(toState))
        {
            Interlocked.Increment(ref Completed);
            if (evt.Warehouse is not null)
                _warehouseDepths.AddOrUpdate(evt.Warehouse, 0, (_, v) => Math.Max(0, v - 1));
        }

        if (FailureStates.Contains(toState))
        {
            Interlocked.Increment(ref Failed);
            if (evt.Warehouse is not null)
                _warehouseDepths.AddOrUpdate(evt.Warehouse, 0, (_, v) => Math.Max(0, v - 1));
        }
    }

    public object[] GetPipelineCounts() =>
        _pipelineCounts.Select(kv => new { state = kv.Key, count = kv.Value })
            .Where(x => x.count > 0)
            .ToArray<object>();

    public object[] GetWarehouseDepths() =>
        _warehouseDepths.Select(kv => new { warehouseId = kv.Key, depth = kv.Value })
            .ToArray<object>();
}
