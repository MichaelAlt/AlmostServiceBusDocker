namespace AzureServiceBusEmulator.Core.Broker;

/// <summary>
/// Message envelope used internally by the emulator broker.
/// </summary>
public sealed class BrokeredMessage
{
    public string MessageId { get; set; } = Guid.NewGuid().ToString();

    public byte[] Body { get; set; } = [];

    public string? ContentType { get; set; }

    public string? CorrelationId { get; set; }

    public string? SessionId { get; set; }

    public string? PartitionKey { get; set; }

    public string? Subject { get; set; }

    public string? ReplyTo { get; set; }

    public string? ReplyToSessionId { get; set; }

    public string? To { get; set; }

    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }

    public TimeSpan TimeToLive { get; set; } = TimeSpan.MaxValue;

    public Dictionary<string, object> ApplicationProperties { get; set; } = [];

    public long SequenceNumber { get; set; }

    public int DeliveryCount { get; set; }

    public string? DeadLetterReason { get; set; }

    public string? DeadLetterErrorDescription { get; set; }

    public DateTimeOffset EnqueuedTimeUtc { get; set; } = DateTimeOffset.UtcNow;

    public string? LockToken { get; set; }

    /// <summary>
    /// Creates an independent copy of this message.
    /// DeliveryCount and SequenceNumber are reset to 0.
    /// ApplicationProperties are deep-copied.
    /// </summary>
    public BrokeredMessage Clone()
    {
        return new BrokeredMessage
        {
            MessageId = MessageId,
            Body = Body,
            ContentType = ContentType,
            CorrelationId = CorrelationId,
            SessionId = SessionId,
            PartitionKey = PartitionKey,
            Subject = Subject,
            ReplyTo = ReplyTo,
            ReplyToSessionId = ReplyToSessionId,
            To = To,
            ScheduledEnqueueTimeUtc = ScheduledEnqueueTimeUtc,
            TimeToLive = TimeToLive,
            LockToken = LockToken,
            DeadLetterReason = DeadLetterReason,
            DeadLetterErrorDescription = DeadLetterErrorDescription,
            EnqueuedTimeUtc = EnqueuedTimeUtc,
            // Reset tracking state
            SequenceNumber = 0,
            DeliveryCount = 0,
            // Deep copy application properties
            ApplicationProperties = new Dictionary<string, object>(ApplicationProperties)
        };
    }
}
