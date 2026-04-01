namespace AzureServiceBusEmulator.Core.Broker;

/// <summary>
/// The filter type applied by a <see cref="RuleEntity"/>.
/// </summary>
public enum FilterType
{
    TrueFilter,
    FalseFilter,
    SqlFilter,
    CorrelationFilter
}

/// <summary>
/// A subscription rule that determines whether a published message should be
/// delivered to the owning subscription.
/// </summary>
public sealed class RuleEntity
{
    public string Name { get; set; } = string.Empty;

    public FilterType FilterType { get; set; } = FilterType.TrueFilter;

    /// <summary>SQL filter expression, used when <see cref="FilterType"/> is <see cref="FilterType.SqlFilter"/>.</summary>
    public string? SqlExpression { get; set; }

    /// <summary>Correlation ID filter value, used when <see cref="FilterType"/> is <see cref="FilterType.CorrelationFilter"/>.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>Optional SQL action expression executed when the rule matches.</summary>
    public string? ActionExpression { get; set; }

    /// <summary>
    /// Evaluates whether this rule matches the given message.
    /// v1: always returns <see langword="true"/> regardless of filter type.
    /// </summary>
    public bool Matches(BrokeredMessage message) => true;
}
