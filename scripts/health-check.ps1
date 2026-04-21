#Requires -Version 5.1
# -------------------------------------------------------
# Platform Health Check Script
# Verifies all services are running and reachable
# Usage: .\scripts\health-check.ps1
# -------------------------------------------------------

$script:pass = 0
$script:fail = 0

function Invoke-Check {
    param(
        [string]$Name,
        [scriptblock]$Command,
        [string]$Expected
    )

    $label = "Checking $Name...".PadRight(42)
    Write-Host -NoNewline $label

    # Allow native command failures inside the check without terminating the script
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $Command 2>&1 | Out-String
        $exitOk = ($LASTEXITCODE -eq 0 -or $LASTEXITCODE -eq $null)

        if ($output -match [regex]::Escape($Expected)) {
            Write-Host "PASS" -ForegroundColor Green
            $script:pass++
        } else {
            Write-Host "FAIL" -ForegroundColor Red
            Write-Host "  Expected : $Expected"
            $preview = ($output.Trim() -split "`n" | Select-Object -First 2) -join " "
            Write-Host "  Got      : $preview"
            $script:fail++
        }
    } catch {
        Write-Host "FAIL" -ForegroundColor Red
        Write-Host "  Error    : $($_.Exception.Message)"
        $script:fail++
    } finally {
        $ErrorActionPreference = $prev
    }
}

Write-Host ""
Write-Host "======================================================"
Write-Host "  Satellite Platform Health Check"
Write-Host "  $((Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')) UTC"
Write-Host "======================================================"
Write-Host ""

# -------------------------------------------------------
# Kafka
# -------------------------------------------------------
Write-Host "[ Kafka ]"

# kafka-broker-api-versions output starts with the internal broker address
# (kafka:9092), not localhost. Check for "Produce" which is always present.
Invoke-Check "Kafka broker reachable" {
    docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
} "Produce"

foreach ($topic in @(
    "sat.position.raw",
    "sat.tle.raw",
    "sat.events.raw",
    "sat.position.enriched",
    "sat.alerts",
    "sat.batch.trigger"
)) {
    Invoke-Check "Topic: $topic" {
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $topic
    } $topic
}

Write-Host ""

# -------------------------------------------------------
# HDFS
# -------------------------------------------------------
Write-Host "[ HDFS ]"

# Invoke-WebRequest returns raw JSON content; Invoke-RestMethod returns a
# PSObject whose string representation may not contain property names.
Invoke-Check "NameNode running" {
    (Invoke-WebRequest -Uri "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem" -UseBasicParsing).Content
} "CapacityTotal"

Invoke-Check "HDFS /satellite/raw" {
    docker exec namenode hdfs dfs -ls /satellite/raw
} "positions"

Invoke-Check "HDFS /satellite/aggregated" {
    docker exec namenode hdfs dfs -ls /satellite/aggregated
} "daily"

Invoke-Check "HDFS /satellite/reports" {
    docker exec namenode hdfs dfs -ls /satellite/reports
} "drift"

Write-Host ""

# -------------------------------------------------------
# Spark
# -------------------------------------------------------
Write-Host "[ Spark ]"

Invoke-Check "Spark Master UI" {
    (Invoke-WebRequest -Uri "http://localhost:8081/" -UseBasicParsing).Content
} "Spark Master"

# JSON endpoint exposes aliveworkers count
Invoke-Check "Spark Worker registered" {
    (Invoke-WebRequest -Uri "http://localhost:8081/json/" -UseBasicParsing).Content
} "aliveworkers"

Write-Host ""

# -------------------------------------------------------
# Redis
# -------------------------------------------------------
Write-Host "[ Redis ]"

Invoke-Check "Redis PING" {
    docker exec redis redis-cli ping
} "PONG"

Write-Host ""

# -------------------------------------------------------
# Airflow
# -------------------------------------------------------
Write-Host "[ Airflow ]"

# /health returns {"status":"healthy",...}
Invoke-Check "Airflow API health" {
    (Invoke-WebRequest -Uri "http://localhost:8083/health" -UseBasicParsing).Content
} "healthy"

Write-Host ""

# -------------------------------------------------------
# Grafana
# -------------------------------------------------------
Write-Host "[ Grafana ]"

# /api/health returns {"commit":"...","database":"ok","version":"..."}
Invoke-Check "Grafana API health" {
    (Invoke-WebRequest -Uri "http://localhost:3000/api/health" -UseBasicParsing).Content
} "database"

Write-Host ""

# -------------------------------------------------------
# Kafka UI
# -------------------------------------------------------
Write-Host "[ Kafka UI ]"

Invoke-Check "Kafka UI accessible" {
    (Invoke-WebRequest -Uri "http://localhost:8080/api/clusters" -UseBasicParsing).Content
} "satellite-cluster"

# -------------------------------------------------------
# Summary
# -------------------------------------------------------
Write-Host ""
Write-Host "======================================================"
Write-Host -NoNewline "  Results: "
Write-Host -NoNewline "$($script:pass) PASSED" -ForegroundColor Green
Write-Host -NoNewline " | "
Write-Host "$($script:fail) FAILED" -ForegroundColor Red
Write-Host "======================================================"
Write-Host ""

if ($script:fail -gt 0) { exit 1 }
