using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AzureServiceBusEmulator.Aspire.Hosting;

/// <summary>
/// Extension methods for adding the Azure Service Bus Emulator to an Aspire distributed application.
/// </summary>
public static class AzureServiceBusEmulatorBuilderExtensions
{
    /// <summary>
    /// Adds the Azure Service Bus Emulator as an executable resource.
    /// The emulator Host project is built and run via <c>dotnet run</c>.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name (used in the Aspire dashboard and for <c>WithReference</c>).</param>
    /// <param name="hostProjectPath">
    /// Absolute or relative path to the <c>AzureServiceBusEmulator.Host.csproj</c> project.
    /// When <c>null</c>, the method attempts to locate the project relative to the current
    /// working directory by walking up the directory tree looking for the solution root.
    /// </param>
    /// <param name="port">
    /// The port the emulator listens on for AMQP and HTTPS traffic.
    /// When <c>null</c>, Aspire assigns a free port automatically.
    /// </param>
    /// <param name="dashboardPort">
    /// The port the emulator dashboard listens on.
    /// Defaults to <c>15672</c>.
    /// </param>
    /// <returns>A resource builder that can be further configured.</returns>
    public static IResourceBuilder<AzureServiceBusEmulatorResource> AddAzureServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        string name,
        string? hostProjectPath = null,
        int? port = null,
        int dashboardPort = 15672)
    {
        hostProjectPath ??= ResolveHostProjectPath(builder);

        var resource = new AzureServiceBusEmulatorResource(name, ".", dashboardPort);

        var args = new List<object>
        {
            "run",
            "--project",
            hostProjectPath,
            "--no-launch-profile",
        };

        var resourceBuilder = builder.AddResource(resource)
            .WithArgs(args.ToArray())
            .WithEndpoint(port, name: "servicebus", scheme: "tcp")
            .WithEndpoint(dashboardPort, dashboardPort, name: "dashboard", scheme: "http")
            .WithExternalHttpEndpoints();

        // When Aspire allocates the port, pass it to the Host as --Port
        resourceBuilder = resourceBuilder.WithArgs(context =>
        {
            var serviceBusEndpoint = resource.GetEndpoint("servicebus");
            context.Args.Add("--Port");
            context.Args.Add(serviceBusEndpoint.Property(EndpointProperty.Port));

            context.Args.Add("--DashboardPort");
            context.Args.Add(dashboardPort.ToString());
        });

        return resourceBuilder;
    }

    /// <summary>
    /// Tries to find the Host project by looking for the solution root (a directory
    /// containing <c>AzureServiceBusEmulator.sln</c>) starting from the AppHost working
    /// directory and walking upward.
    /// </summary>
    private static string ResolveHostProjectPath(IDistributedApplicationBuilder builder)
    {
        var searchDir = builder.AppHostDirectory;

        var current = new DirectoryInfo(searchDir);
        while (current is not null)
        {
            var candidate = Path.Combine(
                current.FullName,
                "src",
                "AzureServiceBusEmulator.Host",
                "AzureServiceBusEmulator.Host.csproj");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        throw new InvalidOperationException(
            "Could not locate AzureServiceBusEmulator.Host.csproj. " +
            "Pass the path explicitly via the hostProjectPath parameter.");
    }
}
