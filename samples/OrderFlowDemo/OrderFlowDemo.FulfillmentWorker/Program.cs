using MassTransit;
using OrderFlowDemo.FulfillmentWorker.Consumers;
using OrderFlowDemo.ServiceDefaults;

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ReserveInventoryConsumer>();
    x.AddConsumer<PickOrderConsumer>();
    x.AddConsumer<ShipOrderConsumer>();
    x.AddConsumer<OrderShippedConsumer>();

    x.AddInMemoryInboxOutbox();

    x.UsingAzureServiceBus((context, cfg) =>
    {
        cfg.Host(builder.Configuration.GetConnectionString("servicebus"));

        // Configure session queue for logistics dispatch (FIFO per warehouse)
        cfg.ReceiveEndpoint("logistics-dispatch", e =>
        {
            e.RequiresSession = true;
            e.ConfigureConsumer<ShipOrderConsumer>(context);
        });

        cfg.ConfigureEndpoints(context);
    });
});

var host = builder.Build();
host.Run();
