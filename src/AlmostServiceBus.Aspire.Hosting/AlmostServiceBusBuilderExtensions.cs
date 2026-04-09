using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AlmostServiceBus.Aspire.Hosting;

/// <summary>
/// Extension methods for adding AlmostServiceBus to an Aspire distributed application.
/// </summary>
public static class AlmostServiceBusBuilderExtensions
{
    /// <summary>
    /// Adds AlmostServiceBus as an executable resource.
    /// The emulator is launched via <c>dotnet exec</c> using the Host binary
    /// embedded in this NuGet package.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name (used in the Aspire dashboard and for <c>WithReference</c>).</param>
    /// <param name="port">
    /// The port the emulator listens on for AMQP and HTTPS traffic.
    /// When <c>null</c>, Aspire assigns a free port automatically.
    /// </param>
    /// <param name="dashboardPort">
    /// The port the emulator dashboard listens on.
    /// Defaults to <c>15672</c>.
    /// </param>
    /// <returns>A resource builder that can be further configured.</returns>
    public static IResourceBuilder<AlmostServiceBusResource> AddServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        string name,
        int? port = null,
        int dashboardPort = 15672)
    {
        var hostDll = ResolveHostDll();

        var resource = new AlmostServiceBusResource(name, Path.GetDirectoryName(hostDll)!, dashboardPort);

        var args = new List<object>
        {
            "exec",
            hostDll,
            "--no-launch-profile",
        };

        var resourceBuilder = builder.AddResource(resource)
            .WithArgs(args.ToArray())
            .WithEndpoint(port, name: "servicebus", scheme: "tcp", isProxied: false)
            .WithEndpoint(dashboardPort, dashboardPort, name: "dashboard", scheme: "http", isProxied: false)
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
    /// Adds AlmostServiceBus as an executable resource, running a Host project from source.
    /// Use this overload when developing against the emulator source code.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name.</param>
    /// <param name="hostProjectPath">Absolute or relative path to <c>AlmostServiceBus.Host.csproj</c>.</param>
    /// <param name="port">The emulator port. When <c>null</c>, Aspire assigns a free port.</param>
    /// <param name="dashboardPort">The dashboard port. Defaults to <c>15672</c>.</param>
    public static IResourceBuilder<AlmostServiceBusResource> AddServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        string name,
        string hostProjectPath,
        int? port = null,
        int dashboardPort = 15672)
    {
        var resource = new AlmostServiceBusResource(name, ".", dashboardPort);

        var args = new List<object>
        {
            "run",
            "--project",
            hostProjectPath,
            "--no-launch-profile",
        };

        var resourceBuilder = builder.AddResource(resource)
            .WithArgs(args.ToArray())
            .WithEndpoint(port, name: "servicebus", scheme: "tcp", isProxied: false)
            .WithEndpoint(dashboardPort, dashboardPort, name: "dashboard", scheme: "http", isProxied: false)
            .WithExternalHttpEndpoints();

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
    /// Locates the embedded Host DLL. Search order:
    /// 1. NuGet package: tools/ directory relative to this assembly (NuGet layout)
    /// 2. Local dev: obj/host-publish/ relative to the project source
    /// </summary>
    private static string ResolveHostDll()
    {
        const string hostDllName = "AlmostServiceBus.Host.dll";

        // NuGet package layout: this assembly is in lib/net10.0/, Host is in tools/
        var assemblyDir = Path.GetDirectoryName(typeof(AlmostServiceBusBuilderExtensions).Assembly.Location)!;
        var nugetToolsDir = Path.Combine(assemblyDir, "..", "..", "tools", hostDllName);
        if (File.Exists(nugetToolsDir))
            return Path.GetFullPath(nugetToolsDir);

        // Local dev: Host published to obj/host-publish/
        var localPublish = Path.Combine(assemblyDir, "host-publish", hostDllName);
        if (File.Exists(localPublish))
            return Path.GetFullPath(localPublish);

        // Fallback: walk up from assembly looking for the published output
        var current = new DirectoryInfo(assemblyDir);
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, "src", "AlmostServiceBus.Aspire.Hosting", "obj", "host-publish", hostDllName);
            if (File.Exists(candidate))
                return Path.GetFullPath(candidate);
            current = current.Parent;
        }

        throw new InvalidOperationException(
            $"Could not locate {hostDllName}. If consuming via NuGet, ensure the AlmostServiceBus.Aspire.Hosting package is installed correctly. " +
            "If building from source, use the overload that takes a hostProjectPath parameter.");
    }
}
