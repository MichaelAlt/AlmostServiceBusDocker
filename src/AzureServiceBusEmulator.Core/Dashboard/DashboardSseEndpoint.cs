using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Dashboard;

public static class DashboardSseEndpoint
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static IEndpointRouteBuilder MapDashboardSse(
        this IEndpointRouteBuilder app,
        MessageEventBus eventBus)
    {
        app.MapGet("/api/dashboard/events", async (HttpContext httpContext, string? ns, string? entity) =>
        {
            httpContext.Response.ContentType = "text/event-stream";
            httpContext.Response.Headers.CacheControl = "no-cache";
            httpContext.Response.Headers.Connection = "keep-alive";

            // Send an initial comment to flush headers — EventSource fires
            // onopen only after receiving the first bytes of the response.
            await httpContext.Response.WriteAsync(": connected\n\n");
            await httpContext.Response.Body.FlushAsync();

            var reader = eventBus.Subscribe();
            var ct = httpContext.RequestAborted;

            try
            {
                await foreach (var evt in reader.ReadAllAsync(ct))
                {
                    if (ns is not null && !evt.Namespace.Equals(ns, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (entity is not null && !evt.Entity.Equals(entity, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var json = JsonSerializer.Serialize(evt, JsonOptions);

                    await httpContext.Response.WriteAsync($"data: {json}\n\n", ct);
                    await httpContext.Response.Body.FlushAsync(ct);
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                eventBus.Unsubscribe(reader);
            }
        });

        return app;
    }
}
