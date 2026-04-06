# MS Emulator Comparison

Runs Wolverine's Azure Service Bus tests against Microsoft's official emulator
to determine if failures are Wolverine bugs or emulator-specific issues.

## Prerequisites

- Docker
- .NET 10 SDK
- `socat` (for port forwarding)

## Usage

```bash
./run-wolverine-against-ms-emulator.sh
```

This will:
1. Start the MS ASB emulator (Docker containers)
2. Forward port 5673→5672 (Wolverine expects 5673)
3. Run the `tracking_correlation_id_on_everything` tests
4. Run the full Wolverine suite
5. Clean up

## What we're testing

The `tracking_correlation_id_on_everything` test fails against our emulator
with a 1-minute timeout. Messages are `Released` by the Azure SDK's
ServiceBusProcessor instead of being processed. Our standalone tests (14 of
them) prove the emulator handles batch+processor correctly, so the issue is
in Wolverine's handler pipeline interaction.

Running against the MS emulator tells us whether this is:
- A Wolverine bug (fails on both emulators)
- An AMQP compatibility issue specific to our AMQPNetLite-based server
