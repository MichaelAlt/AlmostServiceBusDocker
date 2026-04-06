using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Shared logger for AMQP components that aren't created via DI.
/// Set <see cref="Factory"/> during startup to enable logging.
/// </summary>
public static class AmqpLog
{
    public static ILoggerFactory Factory { get; set; } = NullLoggerFactory.Instance;

    public static ILogger CreateLogger<T>() => Factory.CreateLogger<T>();
    public static ILogger CreateLogger(string categoryName) => Factory.CreateLogger(categoryName);
}
