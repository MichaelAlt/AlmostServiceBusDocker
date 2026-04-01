using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

/// <summary>
/// Maps Service Bus REST management API endpoints onto an <see cref="IEndpointRouteBuilder"/>.
/// </summary>
public static class ManagementApiEndpoints
{
    private const string AtomXmlContentType = "application/atom+xml;type=entry;charset=utf-8";

    public static IEndpointRouteBuilder MapServiceBusManagementApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry)
    {
        // ── Queue / Topic CRUD ────────────────────────────────────────────────

        // PUT /{entityName}  — create or update queue/topic
        app.MapPut("/{entityName}", async (string entityName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var body = await ReadBodyAsync(request);

            var isTopic = body.Contains("TopicDescription", StringComparison.OrdinalIgnoreCase);
            var isUpdate = request.Headers.ContainsKey("If-Match");

            if (isTopic)
            {
                TopicEntity entity;
                if (isUpdate)
                {
                    var existing = ns.GetTopic(entityName);
                    if (existing is null)
                        return ManagementApiErrors.EntityNotFound(entityName);
                    entity = existing;
                }
                else
                {
                    entity = ns.CreateTopic(entityName);
                }

                ApplyTopicProperties(entity, body);

                var xml = AtomXmlWriter.WriteTopicEntry(entity);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
            else
            {
                QueueEntity entity;
                if (isUpdate)
                {
                    var existing = ns.GetQueue(entityName);
                    if (existing is null)
                        return ManagementApiErrors.EntityNotFound(entityName);
                    entity = existing;
                }
                else
                {
                    entity = ns.CreateQueue(entityName);
                }

                ApplyQueueProperties(entity, body);

                var xml = AtomXmlWriter.WriteQueueEntry(entity);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
        });

        // GET /{entityName}
        app.MapGet("/{entityName}", (string entityName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);

            var queue = ns.GetQueue(entityName);
            if (queue is not null)
                return Results.Content(AtomXmlWriter.WriteQueueEntry(queue), AtomXmlContentType);

            var topic = ns.GetTopic(entityName);
            if (topic is not null)
                return Results.Content(AtomXmlWriter.WriteTopicEntry(topic), AtomXmlContentType);

            return ManagementApiErrors.EntityNotFound(entityName);
        });

        // DELETE /{entityName}
        app.MapDelete("/{entityName}", (string entityName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);

            if (ns.DeleteQueue(entityName) || ns.DeleteTopic(entityName))
                return Results.Ok();

            return ManagementApiErrors.EntityNotFound(entityName);
        });

        // ── Subscription CRUD ─────────────────────────────────────────────────

        // PUT /{topicName}/Subscriptions/{subName}
        app.MapPut("/{topicName}/Subscriptions/{subName}", async (string topicName, string subName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var body = await ReadBodyAsync(request);
            var isUpdate = request.Headers.ContainsKey("If-Match");

            // Ensure topic exists
            var topic = ns.GetTopic(topicName);
            if (topic is null)
                return ManagementApiErrors.EntityNotFound(topicName);

            SubscriptionEntity sub;
            if (isUpdate)
            {
                var existing = ns.GetSubscription(topicName, subName);
                if (existing is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");
                sub = existing;
            }
            else
            {
                sub = topic.AddSubscription(subName);
            }

            ApplySubscriptionProperties(sub, body, ns);

            var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
            return Results.Content(xml, AtomXmlContentType,
                statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
        });

        // GET /{topicName}/Subscriptions/{subName}
        app.MapGet("/{topicName}/Subscriptions/{subName}", (string topicName, string subName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            return Results.Content(AtomXmlWriter.WriteSubscriptionEntry(sub), AtomXmlContentType);
        });

        // DELETE /{topicName}/Subscriptions/{subName}
        app.MapDelete("/{topicName}/Subscriptions/{subName}", (string topicName, string subName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var topic = ns.GetTopic(topicName);
            if (topic is null || !topic.RemoveSubscription(subName))
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            return Results.Ok();
        });

        // GET /{topicName}/Subscriptions  — list all subscriptions for topic
        app.MapGet("/{topicName}/Subscriptions", (string topicName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var topic = ns.GetTopic(topicName);
            if (topic is null)
                return ManagementApiErrors.EntityNotFound(topicName);

            var feed = AtomXmlWriter.WriteSubscriptionFeed(topic.GetSubscriptions());
            return Results.Content(feed, AtomXmlContentType);
        });

        // ── Rule CRUD ─────────────────────────────────────────────────────────

        // PUT /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
        app.MapPut("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", async (
            string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            var body = await ReadBodyAsync(request);
            var isUpdate = request.Headers.ContainsKey("If-Match");

            RuleProperties props;
            try
            {
                props = AtomXmlReader.ReadRuleProperties(body);
            }
            catch
            {
                // Fallback: create a default TrueFilter rule with given name
                props = new RuleProperties(ruleName, FilterType.TrueFilter, null, null, null);
            }

            var rule = new RuleEntity
            {
                Name = ruleName,
                FilterType = props.FilterType,
                SqlExpression = props.SqlExpression,
                CorrelationId = props.CorrelationId,
                ActionExpression = props.ActionExpression
            };
            sub.AddOrUpdateRule(rule);

            var xml = AtomXmlWriter.WriteRuleEntry(rule);
            return Results.Content(xml, AtomXmlContentType,
                statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
        });

        // GET /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
        app.MapGet("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", (
            string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            var rule = sub.GetRule(ruleName);
            if (rule is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");

            return Results.Content(AtomXmlWriter.WriteRuleEntry(rule), AtomXmlContentType);
        });

        // GET /{topicName}/Subscriptions/{subName}/Rules  — list all rules
        app.MapGet("/{topicName}/Subscriptions/{subName}/Rules", (
            string topicName, string subName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            var feed = AtomXmlWriter.WriteRuleFeed(sub.GetRules());
            return Results.Content(feed, AtomXmlContentType);
        });

        // DELETE /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
        app.MapDelete("/{topicName}/Subscriptions/{subName}/Rules/{ruleName}", (
            string topicName, string subName, string ruleName, HttpRequest request) =>
        {
            var ns = ResolveNamespace(request, registry);
            var sub = ns.GetSubscription(topicName, subName);
            if (sub is null)
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

            if (!sub.RemoveRule(ruleName))
                return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");

            return Results.Ok();
        });

        return app;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static NamespaceContext ResolveNamespace(HttpRequest request, NamespaceRegistry registry)
    {
        var host = request.Host.Host ?? string.Empty;
        var namespaceName = host.Split('.')[0];
        return registry.GetOrCreate(namespaceName);
    }

    private static async Task<string> ReadBodyAsync(HttpRequest request)
    {
        request.EnableBuffering();
        using var reader = new System.IO.StreamReader(request.Body, leaveOpen: true);
        var body = await reader.ReadToEndAsync();
        request.Body.Position = 0;
        return body;
    }

    private static void ApplyQueueProperties(QueueEntity entity, string body)
    {
        try
        {
            var props = AtomXmlReader.ReadQueueProperties(body);
            entity.LockDuration = props.LockDuration;
            entity.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
            entity.RequiresSession = props.RequiresSession;
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            entity.MaxDeliveryCount = props.MaxDeliveryCount;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.ForwardTo = props.ForwardTo;
            entity.UserMetadata = props.UserMetadata;
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }

    private static void ApplyTopicProperties(TopicEntity entity, string body)
    {
        try
        {
            var props = AtomXmlReader.ReadTopicProperties(body);
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.UserMetadata = props.UserMetadata;
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }

    private static void ApplySubscriptionProperties(SubscriptionEntity entity, string body, NamespaceContext ns)
    {
        try
        {
            var props = AtomXmlReader.ReadSubscriptionProperties(body);
            entity.LockDuration = props.LockDuration;
            entity.RequiresSession = props.RequiresSession;
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            entity.MaxDeliveryCount = props.MaxDeliveryCount;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.UserMetadata = props.UserMetadata;

            if (props.ForwardTo is not null)
            {
                entity.ForwardTo = props.ForwardTo;
                entity.ResolvedForwardToQueue = ns.GetQueue(props.ForwardTo);
            }
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }
}
