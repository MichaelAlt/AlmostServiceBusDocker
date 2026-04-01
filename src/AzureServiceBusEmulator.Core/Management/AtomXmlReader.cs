using System.Xml;
using System.Xml.Linq;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

// ── Property records ─────────────────────────────────────────────────────────

public record QueueProperties(
    TimeSpan LockDuration,
    long MaxSizeInMegabytes,
    bool RequiresSession,
    TimeSpan DefaultMessageTimeToLive,
    bool DeadLetteringOnMessageExpiration,
    int MaxDeliveryCount,
    bool EnableBatchedOperations,
    string? ForwardTo,
    string? UserMetadata);

public record TopicProperties(
    TimeSpan DefaultMessageTimeToLive,
    long MaxSizeInMegabytes,
    bool EnableBatchedOperations,
    string? UserMetadata);

public record SubscriptionProperties(
    TimeSpan LockDuration,
    bool RequiresSession,
    TimeSpan DefaultMessageTimeToLive,
    bool DeadLetteringOnMessageExpiration,
    int MaxDeliveryCount,
    bool EnableBatchedOperations,
    string? ForwardTo,
    string? UserMetadata);

public record RuleProperties(
    string Name,
    FilterType FilterType,
    string? SqlExpression,
    string? CorrelationId,
    string? ActionExpression);

// ── Reader ───────────────────────────────────────────────────────────────────

/// <summary>
/// Deserializes Service Bus entities from Atom XML format as produced by <see cref="AtomXmlWriter"/>
/// or as returned by the real Azure Service Bus management API.
/// </summary>
public static class AtomXmlReader
{
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";

    // ── Public API ───────────────────────────────────────────────────────────

    public static QueueProperties ReadQueueProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "QueueDescription");
        return new QueueProperties(
            LockDuration: ParseTimeSpan(desc, "LockDuration"),
            MaxSizeInMegabytes: ParseLong(desc, "MaxSizeInMegabytes"),
            RequiresSession: ParseBool(desc, "RequiresSession"),
            DefaultMessageTimeToLive: ParseTimeSpan(desc, "DefaultMessageTimeToLive"),
            DeadLetteringOnMessageExpiration: ParseBool(desc, "DeadLetteringOnMessageExpiration"),
            MaxDeliveryCount: ParseInt(desc, "MaxDeliveryCount"),
            EnableBatchedOperations: ParseBool(desc, "EnableBatchedOperations"),
            ForwardTo: ParseOptionalString(desc, "ForwardTo"),
            UserMetadata: ParseOptionalString(desc, "UserMetadata"));
    }

    public static TopicProperties ReadTopicProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "TopicDescription");
        return new TopicProperties(
            DefaultMessageTimeToLive: ParseTimeSpan(desc, "DefaultMessageTimeToLive"),
            MaxSizeInMegabytes: ParseLong(desc, "MaxSizeInMegabytes"),
            EnableBatchedOperations: ParseBool(desc, "EnableBatchedOperations"),
            UserMetadata: ParseOptionalString(desc, "UserMetadata"));
    }

    public static SubscriptionProperties ReadSubscriptionProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "SubscriptionDescription");
        return new SubscriptionProperties(
            LockDuration: ParseTimeSpan(desc, "LockDuration"),
            RequiresSession: ParseBool(desc, "RequiresSession"),
            DefaultMessageTimeToLive: ParseTimeSpan(desc, "DefaultMessageTimeToLive"),
            DeadLetteringOnMessageExpiration: ParseBool(desc, "DeadLetteringOnMessageExpiration"),
            MaxDeliveryCount: ParseInt(desc, "MaxDeliveryCount"),
            EnableBatchedOperations: ParseBool(desc, "EnableBatchedOperations"),
            ForwardTo: ParseOptionalString(desc, "ForwardTo"),
            UserMetadata: ParseOptionalString(desc, "UserMetadata"));
    }

    public static RuleProperties ReadRuleProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "RuleDescription");

        var filterEl = desc.Element(Sb + "Filter")
            ?? throw new InvalidOperationException("Missing <Filter> element.");

        var xsiType = filterEl.Attribute(Xsi + "type")?.Value
            ?? throw new InvalidOperationException("Missing xsi:type on <Filter>.");

        FilterType filterType = xsiType switch
        {
            "TrueFilter" => FilterType.TrueFilter,
            "FalseFilter" => FilterType.FalseFilter,
            "SqlFilter" => FilterType.SqlFilter,
            "CorrelationFilter" => FilterType.CorrelationFilter,
            _ => FilterType.TrueFilter
        };

        string? sqlExpression = null;
        string? correlationId = null;

        if (filterType == FilterType.SqlFilter)
            sqlExpression = ParseOptionalString(filterEl, "SqlExpression");
        else if (filterType == FilterType.CorrelationFilter)
            correlationId = ParseOptionalString(filterEl, "CorrelationId");

        var actionEl = desc.Element(Sb + "Action");
        string? actionExpression = null;
        if (actionEl is not null)
        {
            var actionType = actionEl.Attribute(Xsi + "type")?.Value;
            if (actionType == "SqlRuleAction")
                actionExpression = ParseOptionalString(actionEl, "SqlExpression");
        }

        var name = ParseString(desc, "Name");

        return new RuleProperties(name, filterType, sqlExpression, correlationId, actionExpression);
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private static XElement ParseDescription(string xml, XName descriptionElementName)
    {
        var doc = XDocument.Parse(xml);
        // The description can be inside <content> in an <entry>, or directly as root (for feeds we'd parse entries separately)
        var desc = doc.Descendants(descriptionElementName).FirstOrDefault()
            ?? throw new InvalidOperationException($"Element <{descriptionElementName.LocalName}> not found in XML.");
        return desc;
    }

    private static string ParseString(XElement parent, string localName) =>
        parent.Element(Sb + localName)?.Value
            ?? throw new InvalidOperationException($"Missing required element <{localName}>.");

    private static string? ParseOptionalString(XElement parent, string localName) =>
        parent.Element(Sb + localName)?.Value;

    private static int ParseInt(XElement parent, string localName) =>
        int.Parse(ParseString(parent, localName));

    private static long ParseLong(XElement parent, string localName) =>
        long.Parse(ParseString(parent, localName));

    private static bool ParseBool(XElement parent, string localName) =>
        bool.Parse(ParseString(parent, localName));

    private static TimeSpan ParseTimeSpan(XElement parent, string localName)
    {
        var value = ParseString(parent, localName);
        if (value == "P10675199DT2H48M5.4775807S")
            return TimeSpan.MaxValue;
        return XmlConvert.ToTimeSpan(value);
    }
}
