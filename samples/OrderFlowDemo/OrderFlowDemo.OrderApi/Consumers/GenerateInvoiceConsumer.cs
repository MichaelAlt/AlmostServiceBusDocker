using MassTransit;
using OrderFlowDemo.Contracts;

namespace OrderFlowDemo.OrderApi.Consumers;

public class GenerateInvoiceConsumer(ILogger<GenerateInvoiceConsumer> logger) : IConsumer<GenerateInvoice>
{
    public async Task Consume(ConsumeContext<GenerateInvoice> context)
    {
        var msg = context.Message;
        logger.LogInformation("Generating invoice for Order {OrderId}, Amount: £{Amount:F2}",
            msg.OrderId, msg.Amount);

        await Task.Delay(Random.Shared.Next(100, 300), context.CancellationToken);

        await context.Publish(new InvoiceGenerated { OrderId = msg.OrderId });
    }
}
