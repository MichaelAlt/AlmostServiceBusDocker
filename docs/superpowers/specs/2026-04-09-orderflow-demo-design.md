# OrderFlow Demo — Design Spec

A demo/test harness for the AlmostServiceBus emulator using MassTransit. Two separate .NET services communicate exclusively via the Service Bus, with a real-time Vue dashboard visualizing the entire system.

## Solution Structure

Separate solution at `samples/OrderFlowDemo/OrderFlowDemo.sln`:

```
samples/OrderFlowDemo/
├── OrderFlowDemo.AppHost/           # Aspire orchestrator
├── OrderFlowDemo.OrderApi/          # API + Sagas + Dashboard (Vite.AspNetCore)
├── OrderFlowDemo.FulfillmentWorker/ # Warehouse consumers (separate process)
├── OrderFlowDemo.Contracts/         # Shared message types
└── OrderFlowDemo.ServiceDefaults/   # Shared Aspire config (Serilog, etc.)
```

## Aspire AppHost

Orchestrates four resources:

- **servicebus** — `builder.AddServiceBusEmulator("servicebus")` via AlmostServiceBus.Aspire.Hosting
- **seq** — `builder.AddSeq("seq")` for structured logging
- **orderapi** — OrderFlowDemo.OrderApi project, references servicebus + seq
- **fulfillment** — OrderFlowDemo.FulfillmentWorker project, references servicebus + seq

## Domain Model

### Order Lifecycle (Full E-Commerce Lite)

```
Submitted → PaymentPending → PaymentCompleted → InventoryReserving → InventoryReserved
  → Picking → Picked → Shipping → Shipped → Delivering → Delivered → Invoiced
```

Failure paths:
- Payment fails → `PaymentFailed` (dead-letter demo)
- Inventory unavailable → `BackOrdered`
- Shipping fails → retry demo

### Messages (OrderFlowDemo.Contracts)

Commands:
- `ProcessPayment` — self-consumed by OrderApi (mock)
- `ShipOrder` — consumed by FulfillmentWorker via session queue (keyed by WarehouseId)
- `GenerateInvoice` — self-consumed by OrderApi (mock)

Events:
- `OrderSubmitted` — published by OrderApi scenario engine
- `PaymentCompleted` / `PaymentFailed` — published by OrderApi
- `InventoryReserved` / `InventoryUnavailable` — published by FulfillmentWorker
- `OrderPicked` — published by FulfillmentWorker
- `OrderShipped` — published by FulfillmentWorker
- `OrderDelivered` — published by FulfillmentWorker
- `InvoiceGenerated` — published by OrderApi

### What Crosses the Bus

| From | Message | To |
|------|---------|----|
| OrderApi | `OrderSubmitted` | FulfillmentWorker |
| OrderApi | `PaymentCompleted` | FulfillmentWorker |
| FulfillmentWorker | `InventoryReserved` | OrderApi (saga) |
| FulfillmentWorker | `OrderPicked` | OrderApi (saga) |
| FulfillmentWorker | `OrderShipped` | OrderApi (saga) |
| FulfillmentWorker | `OrderDelivered` | OrderApi (saga) |

Self-consumed (don't cross bus): `ProcessPayment`, `GenerateInvoice`. Inventory reservation, picking, and shipping are handled inline by FulfillmentWorker consumers (no separate command messages for these).

## Sagas

### OrderStateMachine (OrderApi)

Orchestrates the full order lifecycle. Stored in EF Core InMemory (`OrderSagaDbContext`).

States: `Submitted`, `PaymentPending`, `PaymentCompleted`, `InventoryReserving`, `InventoryReserved`, `Picking`, `Picked`, `Shipping`, `Shipped`, `Delivering`, `Delivered`, `Invoiced`, `PaymentFailed`, `BackOrdered`.

Correlates on `OrderId` (Guid).

### FIFO Session Queue

`logistics-dispatch` queue with `RequiresSession = true`, keyed by `WarehouseId`. Ensures orders to the same warehouse are processed in strict FIFO order. This demonstrates the emulator's session queue support.

## Scenarios

Pre-defined load patterns exposed via API:

| Scenario | Rate | Behavior |
|----------|------|----------|
| **Steady State** | 2-5/sec | ~5% payment failures, 1% inventory issues |
| **Black Friday Burst** | Ramps 1→50/sec over 30s, holds 60s, ramps down | Higher inventory contention |
| **Warehouse Bottleneck** | 5/sec | All orders route to single warehouse (FIFO demo) |
| **Failure Cascade** | 5/sec | 30% payment failures, 20% shipping failures (DLQ demo) |
| **Happy Path** | 1/sec | No failures, clean lifecycle walkthrough |

### Scenario API

- `GET /api/scenarios` — list available scenarios with descriptions
- `GET /api/scenarios/active` — current scenario + runtime stats
- `POST /api/scenarios/{name}/start` — starts a scenario
- `POST /api/scenarios/stop` — stops current scenario

### Scenario Engine

Background service in OrderApi. Generates orders at configured rate with random product names, quantities, customer names, and warehouse assignments (4 warehouses: London-East, Manchester, Birmingham, Edinburgh). Failure rates and delays configurable per scenario definition.

## OrderApi

ASP.NET + MassTransit + Vite.AspNetCore.

### Responsibilities
- Hosts `OrderStateMachine` saga
- Serves Vue dashboard via Vite.AspNetCore
- Runs scenario engine (background service)
- Exposes dashboard data API + SSE endpoint

### Dashboard API Endpoints

- `GET /api/dashboard/stats` — KPI aggregates (total, completed, in-flight, throughput, DLQ count)
- `GET /api/dashboard/pipeline` — order counts per saga state
- `GET /api/dashboard/queues` — queue depths (queries emulator dashboard API)
- `GET /api/dashboard/warehouses` — FIFO lane depths per warehouse
- `GET /api/dashboard/events` — SSE endpoint streaming real-time events

### SSE Real-Time Events

OrderApi maintains an in-memory `Channel<DashboardEvent>` fed by:
1. `IConsumeObserver` on MassTransit — captures every message consumed
2. Saga state transition hooks — push state change events

SSE endpoint (`text/event-stream`) streams to the browser. Single connection drives the entire dashboard.

```typescript
type DashboardEvent = {
  type: 'saga-transition' | 'message-consumed' | 'message-dead-lettered' | 'queue-depth'
  orderId?: string
  fromState?: string
  toState?: string
  warehouse?: string
  queueName?: string
  depth?: number
  timestamp: string
}
```

## FulfillmentWorker

Worker Service + MassTransit. Separate process — all communication via the bus.

### Consumers
- `PaymentCompletedConsumer` → mock inventory reserve (delay) → publishes `InventoryReserved`
- `InventoryReservedConsumer` → mock pick (delay) → publishes `OrderPicked`
- `OrderPickedConsumer` → sends `ShipOrder` to session queue keyed by `WarehouseId`
- `ShipOrderConsumer` → mock ship (delay) → publishes `OrderShipped`
- `OrderShippedConsumer` → mock delivery (delay) → publishes `OrderDelivered`

Mock delays: 100-500ms configurable. Failure rates injected via MassTransit message headers from scenario config.

## Dashboard (Vue)

Vue 3 + TypeScript + Vite, served from OrderApi via Vite.AspNetCore.

### Dependencies
- `chart.js` + `vue-chartjs` — throughput chart, saga doughnut
- Native `EventSource` API — SSE client

### Layout

Light theme, sidebar navigation, card-based layout.

**Header area:** Scenario selector pills (click to start/stop scenarios).

**Metrics row:** 5 KPI cards — Total Orders (red bg), Completed (white), In Flight (teal bg), Throughput (white), Dead Letters (white, red text).

**Main grid (2:1):**
- Left: Order Pipeline (glowing stage nodes with live counts) + throughput stacked area chart
- Right: Saga State Distribution (doughnut chart)

**Bottom grid (3 cols):**
- Queue Depths — horizontal bars per queue with live counts
- Warehouse FIFO Lanes — order blocks per warehouse session, opacity gradient for queue depth
- Live Event Feed — streaming log with color-coded dots, filter pills (All/Completed/Failed/Sessions)

### Color Palette

```css
--charcoal: #363537;    /* sidebar, text */
--red: #c11f1f;         /* failures, accent cards */
--teal: #068d9d;        /* primary accent, positive states */
--bg: #E9ECEC;          /* page background */
--white: #ffffff;       /* card surfaces */
/* Derived for pipeline stages: */
--amber: #c98a2e;       /* inventory */
--purple: #7b5ea7;      /* picking */
--green: #2d8f52;       /* delivered/success */
--blue: #2a7ab5;        /* shipping */
```

### Components

| Component | Data Source | Update Method |
|-----------|-----------|---------------|
| `ScenarioBar` | `GET /api/scenarios` | Fetch on mount, button clicks |
| `MetricCards` | SSE events | Running counters per event |
| `PipelineFlow` | SSE events | Counts per state, CSS transitions |
| `ThroughputChart` | SSE events | Rolling 60s window, Chart.js streaming |
| `SagaDonut` | SSE events | Doughnut recalculated per event |
| `QueueDepths` | SSE events | Animated bar widths |
| `WarehouseFifo` | SSE events | Order blocks per session |
| `LiveFeed` | SSE events | Prepend, cap at ~50 visible |

## Logging

Both services use SerilogTracing → Seq (Aspire-managed):
- `Serilog.Sinks.Seq` for structured logs
- `SerilogTracing` for activity/span tracing
- MassTransit diagnostic source integration

## Mock Integrations

All external integrations are `Task.Delay` with configurable latency and failure probability:
- Payment gateway: 200-800ms delay, configurable failure %
- Inventory system: 100-400ms delay, configurable unavailability %
- Shipping provider: 150-600ms delay, configurable failure %
- Delivery confirmation: 100-300ms delay

## Order Data Generation

Random data per order:
- Customer: random name from small pool
- Products: 1-5 items from a quirky catalogue inspired by the Hitchhiker's Guide universe (e.g. Pan Galactic Gargle Blaster, Babel Fish, Towel (Extra Fluffy), Infinite Improbability Drive, Point-of-View Gun, Sub-Etha Sens-O-Matic, Nutrimatic Cup of Tea, Joo Janta 200 Super-Chromatic Peril Sensitive Sunglasses, etc.)
- Warehouse: assigned from 4 options (London-East, Manchester, Birmingham, Edinburgh)
- Amount: random £10-£500

No database persistence — all in-memory. Orders exist only as saga state.
