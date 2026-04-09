using System.Text.Json;

namespace OrderFlowDemo.OrderApi.Dashboard;

public static class DashboardSseEndpoint
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static IEndpointRouteBuilder MapDashboardSse(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/dashboard/events", async (
            DashboardEventBus eventBus,
            HttpContext httpContext,
            CancellationToken ct) =>
        {
            httpContext.Response.Headers.ContentType = "text/event-stream";
            httpContext.Response.Headers.CacheControl = "no-cache";
            httpContext.Response.Headers.Connection = "keep-alive";

            var reader = eventBus.Subscribe();
            try
            {
                await foreach (var evt in reader.ReadAllAsync(ct))
                {
                    var json = JsonSerializer.Serialize(evt, JsonOpts);
                    await httpContext.Response.WriteAsync($"data: {json}\n\n", ct);
                    await httpContext.Response.Body.FlushAsync(ct);
                }
            }
            finally
            {
                eventBus.Unsubscribe(reader);
            }
        });

        return app;
    }
}
