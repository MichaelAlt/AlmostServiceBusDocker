namespace AlmostServiceBus.Core.Amqp;

public class AmqpServerOptions
{
    public int Port { get; set; } = 5672;
    public string Host { get; set; } = "localhost";
}
