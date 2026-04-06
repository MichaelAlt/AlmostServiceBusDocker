#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Starting Microsoft Azure Service Bus Emulator ==="
cd "$SCRIPT_DIR"
docker compose up -d
echo "Waiting for emulator to be healthy..."
for i in $(seq 1 60); do
    if docker compose exec -T servicebus-emulator curl -sf http://localhost:5300/ > /dev/null 2>&1; then
        echo "MS emulator ready!"
        break
    fi
    sleep 2
done

# Wolverine expects:
# - Port 5672 for AMQP (the default for both MS emulator and our emulator)
# - Port 5300 for management HTTP
# These match the MS emulator's ports.
#
# But Wolverine's Servers.cs uses port 5673, so we need to map:
echo ""
echo "=== MS emulator is on port 5672/5300 ==="
echo "=== Wolverine expects port 5673/5300 ==="
echo "=== Forwarding port 5673 -> 5672 ==="
socat TCP-LISTEN:5673,fork TCP:localhost:5672 &
SOCAT_PID=$!

cd "$REPO_ROOT"

echo ""
echo "=== Building Wolverine tests ==="
git submodule update --init external/wolverine 2>/dev/null || true
dotnet build external/wolverine/src/Transports/Azure/Wolverine.AzureServiceBus.Tests/Wolverine.AzureServiceBus.Tests.csproj \
    -f net10.0 --verbosity quiet

echo ""
echo "=== Running Wolverine tracking_correlation_id_on_everything tests ==="
echo "=== against the Microsoft official ASB emulator ==="
FILTER='FullyQualifiedName~tracking_correlation_id_on_everything'

dotnet test \
    external/wolverine/src/Transports/Azure/Wolverine.AzureServiceBus.Tests/Wolverine.AzureServiceBus.Tests.csproj \
    --no-build \
    -f net10.0 \
    --filter "$FILTER" \
    --verbosity normal

echo ""
echo "=== Full Wolverine suite (for comparison) ==="
FILTER='FullyQualifiedName!~leader_election'
FILTER="$FILTER&FullyQualifiedName!~Bug_1684_separated_handlers_and_conventional_routing"
FILTER="$FILTER&FullyQualifiedName!~Bug_1933_multi_tenant_conventional_routing"
FILTER="$FILTER&FullyQualifiedName!~Bug_2283_purge_session_subscription"
FILTER="$FILTER&FullyQualifiedName!~StatefulResourceSmokeTests.check_negative"

dotnet test \
    external/wolverine/src/Transports/Azure/Wolverine.AzureServiceBus.Tests/Wolverine.AzureServiceBus.Tests.csproj \
    --no-build \
    -f net10.0 \
    --filter "$FILTER" \
    --verbosity quiet

echo ""
echo "=== Cleanup ==="
kill $SOCAT_PID 2>/dev/null || true
cd "$SCRIPT_DIR"
docker compose down
