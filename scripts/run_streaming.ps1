#Requires -Version 5.1
<#
.SYNOPSIS
    Start Kafka producers + both Spark streaming jobs.

.PARAMETER Reset
    Clear HDFS stream checkpoints before starting.
    Use this after modifying streaming job code.

.PARAMETER ResetData
    Clear HDFS checkpoints AND the raw/positions Parquet store (full clean slate).

.EXAMPLE
    .\scripts\run_streaming.ps1
    .\scripts\run_streaming.ps1 -Reset
    .\scripts\run_streaming.ps1 -ResetData
#>
param(
    [switch]$Reset,
    [switch]$ResetData
)

$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path $PSScriptRoot -Parent
$PythonExe   = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

# ── Pre-flight ────────────────────────────────────────────────────────────────

if (-not (Test-Path $PythonExe)) {
    Write-Error ".venv not found. Run .\scripts\install.ps1 first."
    exit 1
}

Write-Host "Checking required Docker services..."
$required = @("kafka", "namenode", "spark-master", "spark-worker", "redis")
foreach ($svc in $required) {
    $state = ""
    try { $state = (docker inspect --format "{{.State.Status}}" $svc) | Out-String } catch {}
    if ($state.Trim() -ne "running") {
        Write-Error "$svc is not running. Run .\scripts\start.ps1 first."
        exit 1
    }
}
Write-Host "  [OK] All services are running"

# ── Kill any existing streaming jobs ─────────────────────────────────────────

Write-Host ""
Write-Host "Stopping any existing streaming jobs in spark-master..."
try {
    docker exec spark-master bash -c "pkill -f orbit_enrichment.py 2>/dev/null; pkill -f anomaly_detection.py 2>/dev/null; true" | Out-Null
} catch {}
Start-Sleep -Seconds 2

# ── HDFS reset ────────────────────────────────────────────────────────────────

if ($Reset -or $ResetData) {
    Write-Host ""
    Write-Host "Clearing HDFS checkpoints..."
    foreach ($path in @("/satellite/checkpoints/enrichment", "/satellite/checkpoints/anomaly-alerts")) {
        try {
            docker exec namenode hdfs dfs -test -e $path 2>$null
            if ($LASTEXITCODE -eq 0) {
                docker exec namenode hdfs dfs -rm -r $path | Out-Null
                Write-Host "  [OK] Removed $path"
            } else {
                Write-Host "  [--] $path not found, skipping"
            }
        } catch {
            Write-Host "  [--] $path not found, skipping"
        }
    }
}

if ($ResetData) {
    Write-Host "Clearing HDFS raw position data..."
    try {
        docker exec namenode hdfs dfs -test -e /satellite/raw/positions 2>$null
        if ($LASTEXITCODE -eq 0) {
            docker exec namenode hdfs dfs -rm -r /satellite/raw/positions | Out-Null
            Write-Host "  [OK] Removed /satellite/raw/positions"
        } else {
            Write-Host "  [--] /satellite/raw/positions not found, skipping"
        }
    } catch {
        Write-Host "  [--] /satellite/raw/positions not found, skipping"
    }
}

# ── Start producers in a new window ──────────────────────────────────────────

Write-Host ""
Write-Host "Starting Kafka producers..."
$producerProc = Start-Process powershell `
    -ArgumentList "-NoExit", "-File", "`"$PSScriptRoot\run_producers.ps1`"" `
    -PassThru
Write-Host "  [OK] Producers window opened (PID $($producerProc.Id))"

# Give Kafka a moment before Spark starts reading
Start-Sleep -Seconds 3

# ── Start orbit_enrichment ────────────────────────────────────────────────────

Write-Host "Starting orbit_enrichment (Spark job 1)..."
docker exec -d spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512m /opt/spark/jobs/streaming/orbit_enrichment.py
Write-Host "  [OK] Submitted - waiting 20s for executor acquisition..."
Start-Sleep -Seconds 20

# ── Start anomaly_detection ───────────────────────────────────────────────────

Write-Host "Starting anomaly_detection (Spark job 2)..."
docker exec -d spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --executor-memory 512m /opt/spark/jobs/streaming/anomaly_detection.py
Write-Host "  [OK] Submitted"

# ── Summary ───────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "======================================================"
Write-Host "  Satellite Tracking - Streaming Platform"
Write-Host ""
Write-Host "  Components:"
Write-Host "    Producers         -> separate window"
Write-Host "    orbit_enrichment  -> spark-master (1 core / 512m)"
Write-Host "    anomaly_detection -> spark-master (1 core / 512m)"
Write-Host ""
Write-Host "  Monitor:"
Write-Host "    Spark UI  : http://localhost:8080"
Write-Host "    Kafka UI  : http://localhost:8085"
Write-Host "    Spark logs: docker logs spark-master -f"
Write-Host ""
Write-Host "  Press Ctrl+C to stop all components"
Write-Host "======================================================"
Write-Host ""

# ── Wait and clean up on Ctrl+C ───────────────────────────────────────────────

try {
    while ($true) { Start-Sleep -Seconds 5 }
} finally {
    Write-Host ""
    Write-Host "Stopping all components..."

    try {
        docker exec spark-master bash -c "pkill -f orbit_enrichment.py; pkill -f anomaly_detection.py; true" | Out-Null
        Write-Host "  [OK] Spark streaming jobs stopped"
    } catch {}

    if ($producerProc -and -not $producerProc.HasExited) {
        $producerProc.Kill()
        Write-Host "  [OK] Producer window closed"
    }

    Write-Host "Done."
}
