using AlmostServiceBus.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var seq = builder.AddSeq("seq")
    .WithLifetime(ContainerLifetime.Persistent)
    .WithDataVolume("orderflow-seq-data");

var servicebus = builder.AddServiceBusEmulator("servicebus", port: 5672);

builder.AddProject<Projects.OrderFlowDemo_OrderApi>("orderapi")
    .WithReference(servicebus)
    .WithReference(seq)
    .WithExternalHttpEndpoints();

builder.AddProject<Projects.OrderFlowDemo_FulfillmentWorker>("fulfillment")
    .WithReference(servicebus)
    .WithReference(seq);

builder.Build().Run();
