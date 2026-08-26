namespace AlmostServiceBus.Core.Amqp;

public static class AmqpPropertiesExtensions
{

    /// <summary>
    /// Safely converts non-string MessageIds and CorrelationIds (like Java's ulong) 
    /// into strings using native setters so internal property getters never crash.
    /// </summary>
    public static void SanitizeProperties(global::Amqp.Message? message)
    {
        if (message?.Properties == null) return;
        try
        {
            var props = message.Properties;

            // Use the library's native GetMessageId() which returns an object (no casting crash)
            var msgId = props.GetMessageId();
            if (msgId != null && msgId is not string)
            {
                props.SetMessageId(msgId.ToString() ?? string.Empty);
            }

            var corrId = props.GetCorrelationId();
            if (corrId != null && corrId is not string)
            {
                props.SetCorrelationId(corrId.ToString() ?? string.Empty);
            }
        }
        catch { }
    }
}
