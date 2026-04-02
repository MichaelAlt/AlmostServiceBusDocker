using System.Text.RegularExpressions;

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

    // ── Additional correlation filter properties ─────────────────────────────

    public string? Subject { get; set; }

    public string? To { get; set; }

    public string? ReplyTo { get; set; }

    public string? SessionId { get; set; }

    public string? ContentType { get; set; }

    /// <summary>Custom properties to match against <see cref="BrokeredMessage.ApplicationProperties"/>.</summary>
    public Dictionary<string, object>? CorrelationFilterProperties { get; set; }

    /// <summary>Optional SQL action expression executed when the rule matches.</summary>
    public string? ActionExpression { get; set; }

    /// <summary>
    /// Evaluates whether this rule matches the given message.
    /// </summary>
    public bool Matches(BrokeredMessage message)
    {
        return FilterType switch
        {
            FilterType.TrueFilter => true,
            FilterType.FalseFilter => false,
            FilterType.CorrelationFilter => MatchesCorrelationFilter(message),
            FilterType.SqlFilter => MatchesSqlFilter(message),
            _ => true
        };
    }

    private bool MatchesCorrelationFilter(BrokeredMessage message)
    {
        if (CorrelationId is not null && !string.Equals(CorrelationId, message.CorrelationId, StringComparison.Ordinal))
            return false;
        if (Subject is not null && !string.Equals(Subject, message.Subject, StringComparison.Ordinal))
            return false;
        if (To is not null && !string.Equals(To, message.To, StringComparison.Ordinal))
            return false;
        if (ReplyTo is not null && !string.Equals(ReplyTo, message.ReplyTo, StringComparison.Ordinal))
            return false;
        if (SessionId is not null && !string.Equals(SessionId, message.SessionId, StringComparison.Ordinal))
            return false;
        if (ContentType is not null && !string.Equals(ContentType, message.ContentType, StringComparison.Ordinal))
            return false;

        // Match custom properties
        if (CorrelationFilterProperties is not null)
        {
            foreach (var (key, value) in CorrelationFilterProperties)
            {
                if (!message.ApplicationProperties.TryGetValue(key, out var msgValue))
                    return false;
                if (!Equals(value, msgValue))
                    return false;
            }
        }

        return true;
    }

    // Regex for simple SQL equality expressions: property = 'value' or property = number
    private static readonly Regex SimpleEqualityPattern = new(
        @"^\s*(\w+)\s*=\s*'([^']*)'\s*$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex NumericEqualityPattern = new(
        @"^\s*(\w+)\s*=\s*(\d+(?:\.\d+)?)\s*$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex TautologyPattern = new(
        @"^\s*1\s*=\s*1\s*$",
        RegexOptions.Compiled);

    private static readonly Regex ContradictionPattern = new(
        @"^\s*1\s*=\s*0\s*$",
        RegexOptions.Compiled);

    private bool MatchesSqlFilter(BrokeredMessage message)
    {
        if (string.IsNullOrWhiteSpace(SqlExpression))
            return true;

        var expr = SqlExpression.Trim();

        // Handle tautologies
        if (TautologyPattern.IsMatch(expr) || expr.Equals("true", StringComparison.OrdinalIgnoreCase))
            return true;

        // Handle contradictions
        if (ContradictionPattern.IsMatch(expr) || expr.Equals("false", StringComparison.OrdinalIgnoreCase))
            return false;

        // Handle property = 'string-value'
        var stringMatch = SimpleEqualityPattern.Match(expr);
        if (stringMatch.Success)
        {
            var property = stringMatch.Groups[1].Value;
            var value = stringMatch.Groups[2].Value;
            return MatchesPropertyValue(message, property, value);
        }

        // Handle property = numeric-value
        var numericMatch = NumericEqualityPattern.Match(expr);
        if (numericMatch.Success)
        {
            var property = numericMatch.Groups[1].Value;
            var value = numericMatch.Groups[2].Value;

            if (message.ApplicationProperties.TryGetValue(property, out var msgValue))
            {
                return string.Equals(msgValue?.ToString(), value, StringComparison.Ordinal);
            }
            return false;
        }

        // Handle property != 'string-value' and property <> 'string-value'
        var notEqualPattern = Regex.Match(expr, @"^\s*(\w+)\s*(?:!=|<>)\s*'([^']*)'\s*$", RegexOptions.IgnoreCase);
        if (notEqualPattern.Success)
        {
            var property = notEqualPattern.Groups[1].Value;
            var value = notEqualPattern.Groups[2].Value;
            return !MatchesPropertyValue(message, property, value);
        }

        // Handle property IS NULL
        var isNullPattern = Regex.Match(expr, @"^\s*(\w+)\s+IS\s+NULL\s*$", RegexOptions.IgnoreCase);
        if (isNullPattern.Success)
        {
            var property = isNullPattern.Groups[1].Value;
            return !message.ApplicationProperties.ContainsKey(property);
        }

        // Handle property IS NOT NULL
        var isNotNullPattern = Regex.Match(expr, @"^\s*(\w+)\s+IS\s+NOT\s+NULL\s*$", RegexOptions.IgnoreCase);
        if (isNotNullPattern.Success)
        {
            var property = isNotNullPattern.Groups[1].Value;
            return message.ApplicationProperties.ContainsKey(property);
        }

        // For unrecognized expressions, default to match (permissive)
        return true;
    }

    /// <summary>
    /// Matches a property name against both system properties and application properties.
    /// </summary>
    private static bool MatchesPropertyValue(BrokeredMessage message, string property, string value)
    {
        // Check system properties first
        var systemValue = property.ToLowerInvariant() switch
        {
            "correlationid" => message.CorrelationId,
            "subject" or "label" => message.Subject,
            "to" => message.To,
            "replyto" => message.ReplyTo,
            "sessionid" => message.SessionId,
            "contenttype" => message.ContentType,
            "messageid" => message.MessageId,
            _ => null
        };

        if (systemValue is not null)
            return string.Equals(systemValue, value, StringComparison.Ordinal);

        // Check application properties
        if (message.ApplicationProperties.TryGetValue(property, out var appValue))
            return string.Equals(appValue?.ToString(), value, StringComparison.Ordinal);

        return false;
    }
}
