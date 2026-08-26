namespace AlmostServiceBus.Core.Hosting;

public static class EmulatorNetwork
{
    public static string GetPublicHost()
    {
        var value = FirstNonEmpty(
            Environment.GetEnvironmentVariable("ASB_PUBLIC_HOST"),
            Environment.GetEnvironmentVariable("ASB_HOST"),
            Environment.GetEnvironmentVariable("SERVICEBUS_EMULATOR_HOST"));

        if (!string.IsNullOrWhiteSpace(value))
            return value.Trim();

        return IsRunningInContainer()
            ? "servicebus-emulator"
            : "localhost";
    }

    public static string GetBindHost()
    {
        var value = FirstNonEmpty(
            Environment.GetEnvironmentVariable("ASB_BIND_HOST"),
            Environment.GetEnvironmentVariable("ASB_HOST"));

        if (!string.IsNullOrWhiteSpace(value))
            return value.Trim();

        return IsRunningInContainer()
            ? "0.0.0.0"
            : "localhost";
    }

    public static bool IsDefaultNamespaceHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
            return true;

        var candidate = host.Trim().TrimEnd('.');
        if (candidate.Contains(':'))
            candidate = candidate.Split(':', 2)[0];

        if (candidate.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || candidate.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || candidate.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
            || candidate.Equals("::1", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        foreach (var alias in GetDefaultNamespaceAliases())
        {
            if (candidate.Equals(alias, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static IEnumerable<string> GetDefaultNamespaceAliases()
    {
        yield return "host.docker.internal";
        yield return "servicebus-emulator";
        yield return "servicebus";

        var configured = Environment.GetEnvironmentVariable("ASB_DEFAULT_NAMESPACE_HOSTS");
        if (string.IsNullOrWhiteSpace(configured))
            yield break;

        foreach (var part in configured.Split(',', ';', ' ', '\t', '\r', '\n'))
        {
            var value = part.Trim();
            if (!string.IsNullOrEmpty(value))
                yield return value;
        }
    }

    private static bool IsRunningInContainer() =>
        string.Equals(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Environment.GetEnvironmentVariable("ASB_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase);

    private static string? FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }
}
