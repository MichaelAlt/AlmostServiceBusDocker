using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AzureServiceBusEmulator.Core.Broker;

/// <summary>
/// An in-memory queue entity backed by a <see cref="Channel{T}"/>.
/// </summary>
public sealed class QueueEntity
{
    private readonly Channel<BrokeredMessage> _channel;
    private readonly ConcurrentDictionary<string, BrokeredMessage> _pending = new();
    private readonly ConcurrentDictionary<string, BrokeredMessage> _allMessages = new();
    private readonly bool _isDeadLetterQueue;
    private QueueEntity? _deadLetterQueue;
    private int _messageCount;

    public QueueEntity(string name, bool isDeadLetterQueue = false)
    {
        Name = name;
        _isDeadLetterQueue = isDeadLetterQueue;

        _channel = Channel.CreateUnbounded<BrokeredMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });
    }

    // --- Configuration properties ---

    public string Name { get; }

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDeliveryCount { get; set; } = 10;

    public bool RequiresSession { get; set; }

    public bool DeadLetteringOnMessageExpiration { get; set; }

    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public bool EnableBatchedOperations { get; set; } = true;

    public long MaxSizeInMegabytes { get; set; } = 1024L;

    public string? ForwardTo { get; set; }

    public string? ForwardDeadLetteredMessagesTo { get; set; }

    public string? UserMetadata { get; set; }

    /// <summary>
    /// Approximate count of messages currently in the queue.
    /// </summary>
    public int MessageCount => _messageCount;

    /// <summary>
    /// The dead-letter queue for this entity. Created lazily.
    /// If this instance is already a dead-letter queue, returns itself.
    /// </summary>
    public QueueEntity DeadLetterQueue
    {
        get
        {
            if (_isDeadLetterQueue)
                return this;

            return _deadLetterQueue ??= new QueueEntity($"{Name}/$deadletterqueue", isDeadLetterQueue: true);
        }
    }

    // --- Operations ---

    /// <summary>
    /// Enqueues a message, assigning a lock token if one is not already set.
    /// </summary>
    public void Enqueue(BrokeredMessage message)
    {
        message.LockToken ??= Guid.NewGuid().ToString();
        _channel.Writer.TryWrite(message);
        _allMessages[message.LockToken!] = message;
        Interlocked.Increment(ref _messageCount);
    }

    /// <summary>
    /// Asynchronously dequeues the next message, incrementing its delivery count
    /// and tracking it in the pending dictionary.
    /// </summary>
    public async ValueTask<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        var message = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Decrement(ref _messageCount);
        message.DeliveryCount++;
        TrackPending(message);
        return message;
    }

    /// <summary>
    /// Non-blocking attempt to dequeue the next message. Returns null if nothing is available.
    /// </summary>
    public BrokeredMessage? TryDequeueImmediate()
    {
        if (_channel.Reader.TryRead(out var message))
        {
            Interlocked.Decrement(ref _messageCount);
            message.DeliveryCount++;
            TrackPending(message);
            return message;
        }

        return null;
    }

    /// <summary>
    /// Adds a message to the pending (locked) dictionary by its lock token.
    /// </summary>
    public void TrackPending(BrokeredMessage message)
    {
        if (message.LockToken is not null)
            _pending[message.LockToken] = message;
    }

    /// <summary>
    /// Completes a message, removing it from the pending dictionary.
    /// </summary>
    public void Complete(string lockToken)
    {
        _pending.TryRemove(lockToken, out _);
        _allMessages.TryRemove(lockToken, out _);
    }

    /// <summary>
    /// Abandons a message. If delivery count has reached <see cref="MaxDeliveryCount"/>,
    /// the message is moved to the dead-letter queue; otherwise it is re-enqueued.
    /// </summary>
    public void Abandon(string lockToken)
    {
        if (!_pending.TryRemove(lockToken, out var message))
            return;

        if (message.DeliveryCount >= MaxDeliveryCount)
        {
            DeadLetter(message, "MaxDeliveryCountExceeded", $"Message delivery count exceeded the maximum of {MaxDeliveryCount}.");
        }
        else
        {
            Enqueue(message);
        }
    }

    /// <summary>
    /// Moves a pending message to the dead-letter queue with the given reason and description.
    /// </summary>
    public void DeadLetter(string lockToken, string? reason, string? description)
    {
        if (!_pending.TryRemove(lockToken, out var message))
            return;

        _allMessages.TryRemove(lockToken, out _);
        DeadLetter(message, reason, description);
    }

    private void DeadLetter(BrokeredMessage message, string? reason, string? description)
    {
        message.DeadLetterReason = reason;
        message.DeadLetterErrorDescription = description;
        DeadLetterQueue.Enqueue(message);
    }

    /// <summary>
    /// Returns a snapshot of messages in the queue without removing them.
    /// </summary>
    public IReadOnlyList<BrokeredMessage> PeekMessages(int maxCount = 50)
    {
        return _allMessages.Values
            .OrderBy(m => m.SequenceNumber)
            .Take(maxCount)
            .ToList()
            .AsReadOnly();
    }
}
