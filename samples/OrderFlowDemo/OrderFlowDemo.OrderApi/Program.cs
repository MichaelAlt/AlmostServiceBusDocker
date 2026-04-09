using MassTransit;
using OrderFlowDemo.OrderApi.Consumers;
using OrderFlowDemo.OrderApi.Dashboard;
using OrderFlowDemo.OrderApi.Sagas;
using OrderFlowDemo.OrderApi.Scenarios;
using OrderFlowDemo.ServiceDefaults;
using Vite.AspNetCore;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

// In-memory saga state (no persistence needed for a demo)

// Dashboard event bus and stats
var dashboardEventBus = new DashboardEventBus();
var dashboardStats = new DashboardStats();
builder.Services.AddSingleton(dashboardEventBus);
builder.Services.AddSingleton(dashboardStats);
builder.Services.AddSingleton(sp => new SagaObserver(
    sp.GetRequiredService<DashboardEventBus>(),
    sp.GetRequiredService<DashboardStats>()));

// MassTransit
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ProcessPaymentConsumer>();
    x.AddConsumer<GenerateInvoiceConsumer>();

    x.AddSagaStateMachine<OrderStateMachine, OrderState>()
        .InMemoryRepository();

    x.AddInMemoryInboxOutbox();

    x.UsingAzureServiceBus((context, cfg) =>
    {
        cfg.Host(builder.Configuration.GetConnectionString("servicebus"));
        cfg.ConnectConsumeObserver(context.GetRequiredService<SagaObserver>());
        cfg.ConfigureEndpoints(context);
    });
});

// Scenario engine
builder.Services.AddSingleton<ScenarioEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ScenarioEngine>());

// Vite — PackageDirectory is set via config (Vite:Server:PackageDirectory)
builder.Configuration["Vite:Server:PackageDirectory"] = "ClientApp";
builder.Services.AddViteServices(options =>
{
    options.Base = "/";
});

// CORS for dev
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();

app.UseCors();

if (app.Environment.IsDevelopment())
{
    app.UseViteDevelopmentServer();
}

app.UseStaticFiles();

app.MapDashboardApi();
app.MapScenarioApi();
app.MapDashboardSse();
app.MapFallbackToFile("index.html");

app.Run();
