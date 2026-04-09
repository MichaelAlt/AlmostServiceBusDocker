# Run the OrderFlow Demo (OrderApi + FulfillmentWorker)
# Press Ctrl+C to stop both processes

$ErrorActionPreference = "Stop"

$scriptDir = $PSScriptRoot

$env:ConnectionStrings__servicebus = "Endpoint=sb://localhost:5672;SharedAccessKeyName=OrderFlowDemo;SharedAccessKey=emulator"

# Build the Vue dashboard so changes are picked up
Write-Host "Building Vue dashboard..." -ForegroundColor Cyan
npm ci --prefix "$scriptDir\OrderFlowDemo.OrderApi\ClientApp"
npm run build --prefix "$scriptDir\OrderFlowDemo.OrderApi\ClientApp"

$orderApi = Start-Process -PassThru -NoNewWindow dotnet -ArgumentList "run", "--project", "$scriptDir\OrderFlowDemo.OrderApi"
$worker = Start-Process -PassThru -NoNewWindow dotnet -ArgumentList "run", "--project", "$scriptDir\OrderFlowDemo.FulfillmentWorker"

Write-Host ""
Write-Host "OrderFlow Demo is running:" -ForegroundColor Green
Write-Host "  OrderApi:          http://localhost:5200" -ForegroundColor Cyan
Write-Host "  FulfillmentWorker: running" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop both processes." -ForegroundColor Yellow

try {
    while (-not $orderApi.HasExited -and -not $worker.HasExited) {
        Start-Sleep -Milliseconds 500
    }
} finally {
    if (-not $orderApi.HasExited) { Stop-Process -Id $orderApi.Id -Force -ErrorAction SilentlyContinue }
    if (-not $worker.HasExited) { Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue }
    Write-Host "Stopped." -ForegroundColor Yellow
}
