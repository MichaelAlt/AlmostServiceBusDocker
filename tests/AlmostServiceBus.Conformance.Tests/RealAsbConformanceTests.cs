using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// Runs all conformance tests against real Azure Service Bus.
/// Only runs when the ASB_CONNECTION_STRING environment variable is set.
/// When the env var is not set, all tests in this class are skipped.
/// </summary>
public class RealAsbConformanceTests : ConformanceTestBase
{
    private static readonly string? ConnectionString =
        Environment.GetEnvironmentVariable("ASB_CONNECTION_STRING");

    protected override Task<(ServiceBusClient? client, ServiceBusAdministrationClient? admin)> CreateClientsAsync()
    {
        if (string.IsNullOrEmpty(ConnectionString))
        {
            SkipReason = "ASB_CONNECTION_STRING environment variable not set — skipping real ASB tests";
            return Task.FromResult<(ServiceBusClient?, ServiceBusAdministrationClient?)>((null, null));
        }

        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 2,
                TryTimeout = TimeSpan.FromSeconds(10)
            }
        };

        var client = new ServiceBusClient(ConnectionString, clientOptions);
        var admin = new ServiceBusAdministrationClient(ConnectionString);

        return Task.FromResult<(ServiceBusClient?, ServiceBusAdministrationClient?)>((client, admin));
    }
}
