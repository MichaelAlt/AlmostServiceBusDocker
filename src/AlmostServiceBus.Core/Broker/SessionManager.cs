using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AlmostServiceBus.Core.Broker;

public class SessionState
{
    public string SessionId { get; }
    public Channel<BrokeredMessage> Messages { get; }
    public string? LockedBy { get; set; }
    public DateTimeOffset LockedUntil { get; set; }
    public byte[]? UserState { get; set; }
    private int _messageCount;

    public int MessageCount => _messageCount;

    public SessionState(string sessionId)
    {
        SessionId = sessionId;
        Messages = Channel.CreateUnbounded<BrokeredMessage>();
    }

    public void IncrementCount() => Interlocked.Increment(ref _messageCount);
    public void DecrementCount() => Interlocked.Decrement(ref _messageCount);

    public bool IsLocked => LockedBy is not null && DateTimeOffset.UtcNow < LockedUntil;
}

public class SessionManager
{
    private readonly ConcurrentDictionary<string, SessionState> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _lockDuration;

    public SessionManager(TimeSpan lockDuration)
    {
        _lockDuration = lockDuration;
    }

    /// <summary>
    /// Routes a message to the appropriate session channel by SessionId.
    /// Creates the session lazily if it doesn't exist.
    /// </summary>
    public void Enqueue(BrokeredMessage message)
    {
        if (string.IsNullOrEmpty(message.SessionId))
            throw new InvalidOperationException("Messages sent to a session-enabled queue must have a SessionId.");

        var session = _sessions.GetOrAdd(message.SessionId, id => new SessionState(id));
        session.Messages.Writer.TryWrite(message);
        session.IncrementCount();
    }

    /// <summary>
    /// Locks a session for exclusive access by a receiver.
    /// If sessionId is null, picks the next available session (one with messages, not locked).
    /// Returns null if no session is available.
    /// </summary>
    public SessionState? TryAcceptSession(string? sessionId, string receiverId)
    {
        if (sessionId is not null)
        {
            if (_sessions.TryGetValue(sessionId, out var specific) && !specific.IsLocked)
            {
                specific.LockedBy = receiverId;
                specific.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
                return specific;
            }
            return null;
        }

        // Next available: find an unlocked session with messages
        foreach (var session in _sessions.Values)
        {
            if (!session.IsLocked && session.MessageCount > 0)
            {
                session.LockedBy = receiverId;
                session.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
                return session;
            }
        }

        return null;
    }

    /// <summary>
    /// Releases the session lock.
    /// </summary>
    public void ReleaseSession(string sessionId)
    {
        if (_sessions.TryGetValue(sessionId, out var session))
        {
            session.LockedBy = null;
            session.LockedUntil = default;
        }
    }

    /// <summary>
    /// Extends the session lock duration.
    /// </summary>
    public DateTimeOffset? RenewSessionLock(string sessionId)
    {
        if (!_sessions.TryGetValue(sessionId, out var session) || !session.IsLocked)
            return null;

        session.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
        return session.LockedUntil;
    }

    public byte[]? GetSessionState(string sessionId)
    {
        return _sessions.TryGetValue(sessionId, out var session) ? session.UserState : null;
    }

    public void SetSessionState(string sessionId, byte[]? state)
    {
        var session = _sessions.GetOrAdd(sessionId, id => new SessionState(id));
        session.UserState = state;
    }

    public IReadOnlyCollection<string> GetAvailableSessionIds()
    {
        return _sessions.Values
            .Where(s => s.MessageCount > 0 && !s.IsLocked)
            .Select(s => s.SessionId)
            .ToList();
    }
}
