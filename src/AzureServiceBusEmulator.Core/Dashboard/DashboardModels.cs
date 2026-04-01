namespace AzureServiceBusEmulator.Core.Dashboard;

public record NamespaceInfo(string Name, int QueueCount, int TopicCount);

public record EntityOverview(
    List<QueueInfo> Queues,
    List<TopicInfo> Topics);

public record QueueInfo(
    string Name,
    int MessageCount,
    int DeadLetterCount,
    int MaxDeliveryCount,
    string? ForwardTo);

public record TopicInfo(
    string Name,
    List<SubscriptionInfo> Subscriptions);

public record SubscriptionInfo(
    string Name,
    string? ForwardTo,
    int MessageCount,
    int RuleCount);

public record MessageInfo(
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? CorrelationId,
    int DeliveryCount,
    DateTimeOffset EnqueuedTimeUtc,
    string? Subject,
    Dictionary<string, object>? ApplicationProperties,
    string? BodyText,
    Dictionary<string, object>? ScalarProperties);
