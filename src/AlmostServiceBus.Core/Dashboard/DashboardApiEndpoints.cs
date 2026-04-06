using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Core.Dashboard;

public static class DashboardApiEndpoints
{
    public static IEndpointRouteBuilder MapDashboardApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry)
    {
        var api = app.MapGroup("/api/dashboard");

        api.MapGet("/namespaces", () =>
        {
            return registry.ListNamespaces().Select(name =>
            {
                var ns = registry.Get(name);
                return new NamespaceInfo(
                    name,
                    ns?.GetQueues().Count ?? 0,
                    ns?.GetTopics().Count ?? 0,
                    ns?.LastActivityAt ?? DateTimeOffset.MinValue);
            }).ToList();
        });

        api.MapGet("/namespaces/{ns}/entities", (string ns) =>
        {
            var context = registry.Get(ns);
            if (context is null) return Results.NotFound();

            var queues = context.GetQueues().Select(q => new QueueInfo(
                q.Name, q.MessageCount, q.DeadLetterQueue.MessageCount,
                q.TotalMessageCount, q.MaxDeliveryCount, q.ForwardTo)).ToList();

            var topics = context.GetTopics().Select(t => new TopicInfo(
                t.Name,
                t.GetSubscriptions().Select(s => new SubscriptionInfo(
                    s.Name, s.ForwardTo,
                    s.Queue.MessageCount,
                    s.GetRules().Count)).ToList()
            )).ToList();

            return Results.Ok(new EntityOverview(queues, topics));
        });

        // Catch-all GET for queue messages and deadletter peek.
        // The {**path} captures "queueName/messages" or "queueName/deadletter".
        api.MapGet("/namespaces/{ns}/queues/{**path}", (string ns, string path) =>
        {
            var context = registry.Get(ns);
            if (context is null) return Results.NotFound();

            if (path.EndsWith("/messages", StringComparison.OrdinalIgnoreCase))
            {
                var queueName = path[..^"/messages".Length];
                var queue = context.GetQueue(queueName);
                if (queue is null) return Results.NotFound();
                return Results.Ok(queue.PeekMessages(50).Select(ToMessageInfo).ToList());
            }

            if (path.EndsWith("/deadletter", StringComparison.OrdinalIgnoreCase))
            {
                var queueName = path[..^"/deadletter".Length];
                var queue = context.GetQueue(queueName);
                if (queue is null) return Results.NotFound();
                return Results.Ok(queue.DeadLetterQueue.PeekMessages(50).Select(ToMessageInfo).ToList());
            }

            return Results.NotFound();
        });

        // Catch-all GET for topic subscription messages.
        api.MapGet("/namespaces/{ns}/topics/{**path}", (string ns, string path) =>
        {
            var context = registry.Get(ns);
            if (context is null) return Results.NotFound();

            // path = "TopicName/subscriptions/SubName/messages"
            // or just "TopicName/messages" (peek all subscriptions)
            if (path.EndsWith("/messages", StringComparison.OrdinalIgnoreCase))
            {
                var topicName = path[..^"/messages".Length];
                var topic = context.GetTopic(topicName);
                if (topic is null) return Results.NotFound();

                // Aggregate messages from all subscriptions' queues
                var messages = topic.GetSubscriptions()
                    .SelectMany(s => s.Queue.PeekMessages(50))
                    .OrderByDescending(m => m.SequenceNumber)
                    .Take(50)
                    .Select(ToMessageInfo)
                    .ToList();
                return Results.Ok(messages);
            }

            return Results.NotFound();
        });

        // Catch-all DELETE for queue purge operations.
        api.MapDelete("/namespaces/{ns}/queues/{**path}", (string ns, string path) =>
        {
            var context = registry.Get(ns);
            if (context is null) return Results.NotFound();

            if (path.EndsWith("/messages", StringComparison.OrdinalIgnoreCase))
            {
                var queueName = path[..^"/messages".Length];
                var queue = context.GetQueue(queueName);
                if (queue is null) return Results.NotFound();
                while (queue.TryDequeueImmediate() is not null) { }
                return Results.Ok();
            }

            if (path.EndsWith("/deadletter", StringComparison.OrdinalIgnoreCase))
            {
                var queueName = path[..^"/deadletter".Length];
                var queue = context.GetQueue(queueName);
                if (queue is null) return Results.NotFound();
                while (queue.DeadLetterQueue.TryDequeueImmediate() is not null) { }
                return Results.Ok();
            }

            return Results.NotFound();
        });

        return app;
    }

    private static MessageInfo ToMessageInfo(BrokeredMessage m)
    {
        string? bodyText = null;
        Dictionary<string, object>? scalars = null;
        if (m.Body is { Length: > 0 })
        {
            bodyText = Encoding.UTF8.GetString(m.Body);
            scalars = ExtractScalars(bodyText);
        }

        return new MessageInfo(
            m.MessageId, m.SequenceNumber, m.ContentType,
            m.CorrelationId, m.DeliveryCount, m.EnqueuedTimeUtc,
            m.Subject, m.ApplicationProperties, bodyText, scalars,
            m.State.ToString());
    }

    private static Dictionary<string, object>? ExtractScalars(string json)
    {
        try
        {
            var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("message", out var inner)) root = inner;
            var result = new Dictionary<string, object>();
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Value.ValueKind is JsonValueKind.String)
                    result[prop.Name] = prop.Value.GetString()!;
                else if (prop.Value.ValueKind is JsonValueKind.Number)
                    result[prop.Name] = prop.Value.GetDouble();
                else if (prop.Value.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result[prop.Name] = prop.Value.GetBoolean();
                if (result.Count >= 5) break;
            }
            return result.Count > 0 ? result : null;
        }
        catch { return null; }
    }
}
