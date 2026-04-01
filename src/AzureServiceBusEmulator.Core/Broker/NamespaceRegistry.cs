using System.Collections.Concurrent;

namespace AzureServiceBusEmulator.Core.Broker;

/// <summary>
/// Top-level registry that maps namespace names to their isolated
/// <see cref="NamespaceContext"/> instances (tenant isolation).
/// </summary>
public sealed class NamespaceRegistry
{
    private static readonly StringComparer KeyComparer = StringComparer.OrdinalIgnoreCase;

    private readonly ConcurrentDictionary<string, NamespaceContext> _namespaces = new(KeyComparer);

    /// <summary>
    /// Returns the <see cref="NamespaceContext"/> for the given namespace name,
    /// creating one if it does not yet exist.
    /// </summary>
    public NamespaceContext GetOrCreate(string namespaceName) =>
        _namespaces.GetOrAdd(namespaceName, n => new NamespaceContext(n));

    /// <summary>
    /// Returns the <see cref="NamespaceContext"/> for the given namespace name,
    /// or <see langword="null"/> if it has not been created yet.
    /// </summary>
    public NamespaceContext? Get(string namespaceName) =>
        _namespaces.GetValueOrDefault(namespaceName);
}
