#Requires -Version 5.1
# -------------------------------------------------------
# Platform Startup Script
# Brings up all services in the correct order
# Usage: .\scripts\start.ps1 [-Reset]
# -------------------------------------------------------

param(
    [switch]$Reset
)

$ErrorActionPreference = 'Stop'

$ComposeFile = "docker\docker-compose.yml"
$EnvFile     = ".env"

# -- Reset confirmation --------------------------------
if ($Reset) {
    Write-Warning "RESET mode: all volumes will be deleted!"
    $confirm = Read-Host "Are you sure? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Aborted."
        exit 0
    }
}

# -- Verify .env exists --------------------------------
if (-not (Test-Path $EnvFile)) {
    Write-Error ".env file not found. Copy .env.example to .env and fill in your API keys."
    exit 1
}

$envContent = Get-Content $EnvFile -Raw
if ($envContent -match "YOUR_N2YO_API_KEY_HERE") {
    Write-Warning "N2YO_API_KEY not set in .env - N2YO producer will be disabled until you add a key."
}

# -- Reset volumes if requested ------------------------
if ($Reset) {
    Write-Host "Stopping and removing all volumes..."
    docker compose -f $ComposeFile down -v --remove-orphans
    Write-Host "  [OK] Volumes removed"
}

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

function Wait-Healthy {
    param(
        [string]$Container,
        [int]$SleepSec  = 5,
        [int]$MaxWaitSec = 120
    )
    Write-Host "  Waiting for $Container to be healthy (max ${MaxWaitSec}s)..."
    $waited = 0
    do {
        Start-Sleep -Seconds $SleepSec
        $waited += $SleepSec
        $prev = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        $status = docker inspect $Container --format "{{.State.Health.Status}}" 2>&1 | Out-String
        $ErrorActionPreference = $prev
        $status = $status.Trim()
        if ($waited -ge $MaxWaitSec -and $status -ne "healthy") {
            Write-Warning "  $Container not healthy after ${MaxWaitSec}s (status: $status) - continuing anyway."
            return
        }
    } while ($status -ne "healthy")
    Write-Host "  [OK] $Container is healthy"
}

function Wait-Exited {
    param(
        [string]$Container,
        [int]$SleepSec   = 3,
        [int]$MaxWaitSec = 120
    )
    Write-Host "  Waiting for $Container to finish (max ${MaxWaitSec}s)..."
    $waited = 0
    do {
        Start-Sleep -Seconds $SleepSec
        $waited += $SleepSec
        $prev = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        $status = docker inspect $Container --format "{{.State.Status}}" 2>&1 | Out-String
        $ErrorActionPreference = $prev
        $status = $status.Trim()
        if ($waited -ge $MaxWaitSec) {
            Write-Warning "  $Container did not exit after ${MaxWaitSec}s - continuing anyway."
            return
        }
    } while ($status -ne "exited")
    Write-Host "  [OK] $Container finished"
}

# -------------------------------------------------------

Write-Host ""
Write-Host "======================================================"
Write-Host "  Starting Satellite Tracking Platform"
Write-Host "======================================================"
Write-Host ""

# -- Step 1: Zookeeper + Kafka -------------------------
Write-Host "Step 1/6: Starting Zookeeper & Kafka..."
docker compose -f $ComposeFile up -d zookeeper kafka
Wait-Healthy -Container "kafka" -MaxWaitSec 120

# -- Step 2: Kafka topics ------------------------------
Write-Host ""
Write-Host "Step 2/6: Initializing Kafka topics..."
docker compose -f $ComposeFile up --no-deps kafka-init
Write-Host "  [OK] Topics created"

# -- Step 3: Hadoop ------------------------------------
Write-Host ""
Write-Host "Step 3/6: Starting Hadoop (NameNode + DataNode)..."
docker compose -f $ComposeFile up -d namenode
Wait-Healthy -Container "namenode" -SleepSec 8 -MaxWaitSec 180
docker compose -f $ComposeFile up -d datanode
Start-Sleep -Seconds 8

# -- Step 4: HDFS directories --------------------------
Write-Host ""
Write-Host "Step 4/6: Initializing HDFS directories..."
docker compose -f $ComposeFile up --no-deps hdfs-init
Write-Host "  [OK] HDFS directories created"

# -- Step 5: Spark + Redis + Kafka UI ------------------
Write-Host ""
Write-Host "Step 5/6: Starting Spark, Redis, Kafka UI..."
docker compose -f $ComposeFile up -d spark-master spark-worker redis kafka-ui
Wait-Healthy -Container "spark-master" -MaxWaitSec 60
Wait-Healthy -Container "redis"        -MaxWaitSec 30

# -- Step 6: Airflow + Grafana -------------------------
Write-Host ""
Write-Host "Step 6/6: Starting Airflow & Grafana..."
docker compose -f $ComposeFile up -d postgres
Wait-Healthy -Container "postgres" -MaxWaitSec 60

docker compose -f $ComposeFile up -d airflow-init
Wait-Exited -Container "airflow-init" -MaxWaitSec 180

docker compose -f $ComposeFile up -d airflow-webserver airflow-scheduler grafana
Wait-Healthy -Container "airflow-webserver" -MaxWaitSec 120
Wait-Healthy -Container "grafana"           -MaxWaitSec 60

# -- Step 6: Serving Layer ----------------------------------
Write-Host "Step 6: Starting serving layer..."
docker compose -f $ComposeFile up -d satellite-api kafka-redis-bridge
Wait-Healthy -Container "satellite-api" -MaxWaitSec 60

# -- Summary -------------------------------------------
Write-Host ""
Write-Host "======================================================"
Write-Host "  All services started!"
Write-Host ""
Write-Host "  Service URLs:"
Write-Host "  +-----------------------+------------------------------+"
Write-Host "  | Kafka UI              | http://localhost:8080         |"
Write-Host "  | HDFS NameNode UI      | http://localhost:9870         |"
Write-Host "  | Spark Master UI       | http://localhost:8081         |"
Write-Host "  | Spark Worker UI       | http://localhost:8082         |"
Write-Host "  | Airflow UI            | http://localhost:8083         |"
Write-Host "  | Grafana               | http://localhost:3000         |"
Write-Host "  | Satellite API         | http://localhost:8084         |"
Write-Host "  | API Docs (Swagger)    | http://localhost:8084/docs    |"
Write-Host "  +-----------------------+------------------------------+"
Write-Host ""
Write-Host "  Credentials (Airflow + Grafana): admin / admin"
Write-Host ""
Write-Host "  Run health check: .\scripts\health-check.ps1"
Write-Host "======================================================"
