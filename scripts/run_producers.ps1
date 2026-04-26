#Requires -Version 5.1
# -------------------------------------------------------
# Run all Kafka producers (ISS, N2YO, DONKI, TLE)
# Usage: .\scripts\run_producers.ps1
#
# Prerequisites:
#   1. Platform running:   .\scripts\start.ps1
#   2. Venv installed:     .\scripts\install.ps1
#   3. .env configured:    N2YO_API_KEY + NASA_API_KEY set
#
# Producers connect to Kafka at localhost:29092 (external listener).
# Press Ctrl+C to stop all producers gracefully.
# -------------------------------------------------------

$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path $PSScriptRoot -Parent
$PythonExe   = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$EnvFile     = Join-Path $ProjectRoot ".env"
$LogsDir     = Join-Path $ProjectRoot "logs"

# ── Pre-flight checks ─────────────────────────────────────

if (-not (Test-Path $EnvFile)) {
    Write-Error ".env not found. Run .\scripts\install.ps1 first."
    exit 1
}

if (-not (Test-Path $PythonExe)) {
    Write-Error ".venv not found. Run .\scripts\install.ps1 first."
    exit 1
}

# Warn if API keys look like placeholders
$envContent = Get-Content $EnvFile -Raw
if ($envContent -match "YOUR_N2YO_API_KEY_HERE") {
    Write-Warning "N2YO_API_KEY is not set. N2YO producer will fail. Edit .env and add your key."
}
if ($envContent -match "NASA_API_KEY=DEMO_KEY") {
    Write-Warning "NASA_API_KEY is DEMO_KEY (30 req/hr limit). DONKI producer will still work."
}

# Check that Kafka is reachable before starting
Write-Host "Checking Kafka connectivity (localhost:29092)..."
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$kafkaCheck = docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1 | Out-String
$ErrorActionPreference = $prev

if ($kafkaCheck -match "Produce") {
    Write-Host "  [OK] Kafka is reachable"
} else {
    Write-Warning "  Kafka does not appear to be running."
    Write-Host "  Start the platform first: .\scripts\start.ps1"
    Write-Host "  Producers will retry on connection failure — continuing anyway."
}

# Ensure logs directory exists (producers write producers.log here)
if (-not (Test-Path $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir | Out-Null
    Write-Host "  [OK] Created logs\ directory"
}

# ── Launch ────────────────────────────────────────────────

Write-Host ""
Write-Host "======================================================"
Write-Host "  Satellite Tracking — Kafka Producers"
Write-Host ""
Write-Host "  Producers:"
Write-Host "    ISS    -> sat.position.raw  (every 5s)"
Write-Host "    N2YO   -> sat.position.raw  (every 15s, 3 satellites)"
Write-Host "    DONKI  -> sat.events.raw    (every 5 min)"
Write-Host "    TLE    -> sat.tle.raw       (every 1 hr)"
Write-Host ""
Write-Host "  Kafka:   localhost:29092"
Write-Host "  Logs:    logs\producers.log"
Write-Host ""
Write-Host "  Press Ctrl+C to stop"
Write-Host "======================================================"
Write-Host ""

# Run from project root so relative paths (logs/, .env) resolve correctly
Set-Location $ProjectRoot

& $PythonExe -m producers.main
