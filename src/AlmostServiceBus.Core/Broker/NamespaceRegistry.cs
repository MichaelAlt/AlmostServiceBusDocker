using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Top-level registry that maps namespace names to their isolated
/// <see cref="NamespaceContext"/> instances (tenant isolation).
/// </summary>
public sealed class NamespaceRegistry
{
    private static readonly StringComparer KeyComparer = StringComparer.OrdinalIgnoreCase;

    private readonly ConcurrentDictionary<string, NamespaceContext> _namespaces = new(KeyComparer);
    private readonly MessageEventBus? _eventBus;

    public NamespaceRegistry(MessageEventBus? eventBus = null)
    {
        _eventBus = eventBus;
    }

    /// <summary>
    /// Returns the <see cref="NamespaceContext"/> for the given namespace name,
    /// creating one if it does not yet exist.
    /// </summary>
    public NamespaceContext GetOrCreate(string namespaceName)
    {
        var isNew = false;
        var ctx = _namespaces.GetOrAdd(namespaceName, n =>
        {
            isNew = true;
            return new NamespaceContext(n, _eventBus);
        });
        if (isNew)
        {
            _eventBus?.Publish(new MessageEvent(
                MessageEventType.NamespaceCreated, namespaceName, "",
                "", 0, null, null, null, DateTimeOffset.UtcNow));
        }
        return ctx;
    }

    /// <summary>
    /// Returns the <see cref="NamespaceContext"/> for the given namespace name,
    /// or <see langword="null"/> if it has not been created yet.
    /// </summary>
    public NamespaceContext? Get(string namespaceName) =>
        _namespaces.GetValueOrDefault(namespaceName);

    /// <summary>
    /// Returns the names of all registered namespaces.
    /// </summary>
    public IReadOnlyCollection<string> ListNamespaces() =>
        _namespaces.Keys.ToList().AsReadOnly();
}
