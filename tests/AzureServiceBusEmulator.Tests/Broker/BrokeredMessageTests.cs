using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class BrokeredMessageTests
{
    [Fact]
    public void Constructor_SetsDefaults()
    {
        var message = new BrokeredMessage();

        Assert.False(string.IsNullOrEmpty(message.MessageId));
        Assert.Equal(0, message.DeliveryCount);
        Assert.NotNull(message.ApplicationProperties);
    }

    [Fact]
    public void Constructor_MessageId_IsValidGuid()
    {
        var message = new BrokeredMessage();

        Assert.True(Guid.TryParse(message.MessageId, out _));
    }

    [Fact]
    public void Constructor_TimeToLive_DefaultsToMaxValue()
    {
        var message = new BrokeredMessage();

        Assert.Equal(TimeSpan.MaxValue, message.TimeToLive);
    }

    [Fact]
    public void Constructor_EnqueuedTimeUtc_IsSetToApproximatelyNow()
    {
        var before = DateTimeOffset.UtcNow;
        var message = new BrokeredMessage();
        var after = DateTimeOffset.UtcNow;

        Assert.True(message.EnqueuedTimeUtc >= before);
        Assert.True(message.EnqueuedTimeUtc <= after);
    }

    [Fact]
    public void SequenceNumber_CanBeAssigned()
    {
        var message = new BrokeredMessage();

        message.SequenceNumber = 42L;

        Assert.Equal(42L, message.SequenceNumber);
    }

    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var original = new BrokeredMessage
        {
            Body = [1, 2, 3],
            ContentType = "application/json",
            CorrelationId = "corr-1",
            SessionId = "session-1",
            PartitionKey = "pk-1",
            Subject = "subject-1",
            ReplyTo = "replyto-1",
            ReplyToSessionId = "rts-1",
            To = "to-1",
            ScheduledEnqueueTimeUtc = DateTimeOffset.UtcNow.AddMinutes(5),
            TimeToLive = TimeSpan.FromMinutes(10),
            LockToken = "lock-abc"
        };
        original.ApplicationProperties["key1"] = "value1";
        original.ApplicationProperties["key2"] = 42;
        original.SequenceNumber = 99L;
        original.DeliveryCount = 3;

        var clone = original.Clone();

        // Identity should be copied
        Assert.Equal(original.MessageId, clone.MessageId);
        Assert.Equal(original.Body, clone.Body);
        Assert.Equal(original.ContentType, clone.ContentType);
        Assert.Equal(original.CorrelationId, clone.CorrelationId);
        Assert.Equal(original.SessionId, clone.SessionId);
        Assert.Equal(original.PartitionKey, clone.PartitionKey);
        Assert.Equal(original.Subject, clone.Subject);
        Assert.Equal(original.ReplyTo, clone.ReplyTo);
        Assert.Equal(original.ReplyToSessionId, clone.ReplyToSessionId);
        Assert.Equal(original.To, clone.To);
        Assert.Equal(original.ScheduledEnqueueTimeUtc, clone.ScheduledEnqueueTimeUtc);
        Assert.Equal(original.TimeToLive, clone.TimeToLive);
        // LockToken is intentionally NOT copied — each queue assigns a fresh one
        Assert.Null(clone.LockToken);

        // DeliveryCount and SequenceNumber should be reset
        Assert.Equal(0, clone.DeliveryCount);
        Assert.Equal(0L, clone.SequenceNumber);

        // ApplicationProperties should be copied with same values
        Assert.Equal("value1", clone.ApplicationProperties["key1"]);
        Assert.Equal(42, clone.ApplicationProperties["key2"]);

        // ApplicationProperties should be independent (different dictionary instance)
        clone.ApplicationProperties["key1"] = "changed";
        Assert.Equal("value1", original.ApplicationProperties["key1"]);
    }

    [Fact]
    public void Clone_ApplicationProperties_AreDeepCopied()
    {
        var original = new BrokeredMessage();
        original.ApplicationProperties["x"] = "original";

        var clone = original.Clone();
        clone.ApplicationProperties["x"] = "modified";
        clone.ApplicationProperties["y"] = "new";

        Assert.Equal("original", original.ApplicationProperties["x"]);
        Assert.False(original.ApplicationProperties.ContainsKey("y"));
    }

    [Fact]
    public void DeadLetterReason_And_Description_DefaultToNull()
    {
        var message = new BrokeredMessage();

        Assert.Null(message.DeadLetterReason);
        Assert.Null(message.DeadLetterErrorDescription);
    }

    [Fact]
    public void NullableStringProperties_DefaultToNull()
    {
        var message = new BrokeredMessage();

        Assert.Null(message.ContentType);
        Assert.Null(message.CorrelationId);
        Assert.Null(message.SessionId);
        Assert.Null(message.PartitionKey);
        Assert.Null(message.Subject);
        Assert.Null(message.ReplyTo);
        Assert.Null(message.ReplyToSessionId);
        Assert.Null(message.To);
        Assert.Null(message.LockToken);
        Assert.Null(message.ScheduledEnqueueTimeUtc);
    }
}
