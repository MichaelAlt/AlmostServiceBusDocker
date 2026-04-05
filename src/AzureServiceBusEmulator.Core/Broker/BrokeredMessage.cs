namespace AzureServiceBusEmulator.Core.Broker;

public enum MessageState
{
    Active,
    Consumed,
    DeadLettered
}

/// <summary>
/// Message envelope used internally by the emulator broker.
/// </summary>
public sealed class BrokeredMessage
{
    public MessageState State { get; set; } = MessageState.Active;
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

    public string? DeadLetterSource { get; set; }

    public DateTimeOffset EnqueuedTimeUtc { get; set; } = DateTimeOffset.UtcNow;

    public string? LockToken { get; set; }

    /// <summary>
    /// UTC time at which the current lock on this message expires.
    /// After this time, settlement operations (Complete, Abandon, DeadLetter) should fail
    /// with a lock-lost error.
    /// </summary>
    public DateTimeOffset LockedUntil { get; set; }

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
            // LockToken intentionally NOT copied — each queue assigns a fresh one on Enqueue.
            // Copying it causes duplicate delivery tags when the same message is cloned
            // to multiple subscriptions forwarding to the same queue.
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
