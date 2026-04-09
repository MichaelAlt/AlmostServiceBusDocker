using OrderFlowDemo.OrderApi.Scenarios;

namespace OrderFlowDemo.OrderApi.Dashboard;

public static class DashboardEndpoints
{
    public static IEndpointRouteBuilder MapDashboardApi(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/dashboard");

        group.MapGet("/stats", (DashboardStats stats, ScenarioEngine engine) =>
            Results.Ok(new
            {
                total = stats.Total,
                completed = stats.Completed,
                failed = stats.Failed,
                inFlight = stats.Total - stats.Completed - stats.Failed,
                ordersPerSecond = engine.ActiveScenario?.OrdersPerSecond ?? 0,
                scenario = engine.ActiveScenario?.Name,
            }));

        group.MapGet("/pipeline", (DashboardStats stats) =>
            Results.Ok(stats.GetPipelineCounts()));

        group.MapGet("/warehouses", (DashboardStats stats) =>
            Results.Ok(stats.GetWarehouseDepths()));

        return app;
    }

    public static IEndpointRouteBuilder MapScenarioApi(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/scenarios");

        group.MapGet("/", () => Results.Ok(
            ScenarioRegistry.All.Values.Select(s => new
            {
                s.Name,
                s.Description,
                s.OrdersPerSecond,
            })));

        group.MapGet("/active", (ScenarioEngine engine) => Results.Ok(new
        {
            scenario = engine.ActiveScenario?.Name,
            description = engine.ActiveScenario?.Description,
            orderCount = engine.OrderCount,
            startedAt = engine.ActiveScenario != null ? engine.StartedAt : (DateTimeOffset?)null,
        }));

        group.MapPost("/{name}/start", (string name, ScenarioEngine engine) =>
        {
            if (!ScenarioRegistry.All.TryGetValue(name, out var scenario))
                return Results.NotFound(new { error = $"Unknown scenario: {name}" });

            engine.Start(scenario);
            return Results.Ok(new { started = name });
        });

        group.MapPost("/stop", (ScenarioEngine engine) =>
        {
            engine.Stop();
            return Results.Ok(new { stopped = true });
        });

        return app;
    }
}
