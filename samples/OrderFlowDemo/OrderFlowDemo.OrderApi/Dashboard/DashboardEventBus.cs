using System.Threading.Channels;

namespace OrderFlowDemo.OrderApi.Dashboard;

public record DashboardEvent
{
    public required string Type { get; init; } // saga-transition, message-consumed, message-dead-lettered
    public Guid? OrderId { get; init; }
    public string? FromState { get; init; }
    public string? ToState { get; init; }
    public string? Warehouse { get; init; }
    public string? CustomerName { get; init; }
    public string[]? Products { get; init; }
    public decimal? Amount { get; init; }
    public string? QueueName { get; init; }
    public string? FailureReason { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

public class DashboardEventBus
{
    private readonly List<Channel<DashboardEvent>> _subscribers = [];
    private readonly Lock _lock = new();

    public void Publish(DashboardEvent evt)
    {
        lock (_lock)
        {
            foreach (var channel in _subscribers)
            {
                channel.Writer.TryWrite(evt);
            }
        }
    }

    public ChannelReader<DashboardEvent> Subscribe()
    {
        var channel = Channel.CreateBounded<DashboardEvent>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        lock (_lock)
        {
            _subscribers.Add(channel);
        }

        return channel.Reader;
    }

    public void Unsubscribe(ChannelReader<DashboardEvent> reader)
    {
        lock (_lock)
        {
            _subscribers.RemoveAll(c => c.Reader == reader);
        }
    }
}
