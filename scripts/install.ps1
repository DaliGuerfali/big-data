#Requires -Version 5.1
# -------------------------------------------------------
# Satellite Tracker - Installation Script
# Installs all prerequisites and sets up the project
# Usage: .\scripts\install.ps1 [-SkipDockerPull]
#
# Run once before starting the platform for the first time.
# Safe to re-run - all steps are idempotent.
# -------------------------------------------------------

param(
    [switch]$SkipDockerPull
)

$ErrorActionPreference = 'Stop'

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

function Write-Header {
    param([string]$Text)
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
}

function Write-Step {
    param([string]$Text)
    Write-Host ""
    Write-Host ">> $Text" -ForegroundColor Yellow
}

function Write-OK {
    param([string]$Text)
    Write-Host "  [OK] $Text" -ForegroundColor Green
}

function Write-Skip {
    param([string]$Text)
    Write-Host "  [--] $Text (already installed, skipping)" -ForegroundColor DarkGray
}

function Write-Warn {
    param([string]$Text)
    Write-Host "  [!!] $Text" -ForegroundColor Yellow
}

function Write-Fail {
    param([string]$Text)
    Write-Host "  [XX] $Text" -ForegroundColor Red
}

function Test-CommandExists {
    param([string]$Command)
    return [bool](Get-Command $Command -ErrorAction SilentlyContinue)
}

# -------------------------------------------------------
# Check admin rights
# -------------------------------------------------------
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal(
    [Security.Principal.WindowsIdentity]::GetCurrent()
)
$isAdmin = $currentPrincipal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)

Write-Header "Satellite Tracking Platform - Installer"
Write-Host "  Date : $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))"
Write-Host "  User : $env:USERNAME"
Write-Host "  Admin: $isAdmin"


# ======================================================
# PHASE 1: System Prerequisites
# ======================================================
Write-Header "Phase 1/4 - System Prerequisites"

# -- winget --------------------------------------------
Write-Step "Checking winget (Windows Package Manager)..."
if (-not (Test-CommandExists "winget")) {
    Write-Fail "winget not found."
    Write-Host ""
    Write-Host "  winget ships with Windows 11 and Windows 10 build 1809 or later." -ForegroundColor White
    Write-Host "  If missing, install 'App Installer' from the Microsoft Store:" -ForegroundColor White
    Write-Host "  https://apps.microsoft.com/detail/9NBLGGH4NNS1" -ForegroundColor White
    Write-Host ""
    Read-Host "  Press Enter to exit, install winget, then re-run this script"
    exit 1
}
Write-OK "winget $(winget --version)"

# -- Docker Desktop ------------------------------------
Write-Step "Checking Docker Desktop..."
$dockerInstalled = Test-CommandExists "docker"
$dockerRunning   = $false

if ($dockerInstalled) {
    # Temporarily allow non-zero exit codes so a stopped Docker doesn't throw
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $dockerInfo = docker info 2>&1 | Out-String
    $dockerExitCode = $LASTEXITCODE
    $ErrorActionPreference = $prev

    if ($dockerExitCode -eq 0) {
        $dockerRunning = $true
        $dockerVer = (docker --version) -replace "Docker version ", "" -replace ",.*", ""
        Write-Skip "Docker $dockerVer"
    } else {
        Write-Warn "Docker is installed but not running."
    }
} else {
    Write-Warn "Docker Desktop not found. Installing via winget..."
    if (-not $isAdmin) {
        Write-Fail "Admin rights required to install Docker Desktop."
        Write-Host "  Re-run as Administrator, or install Docker Desktop manually:" -ForegroundColor White
        Write-Host "  https://www.docker.com/products/docker-desktop/" -ForegroundColor White
        exit 1
    }
    winget install -e --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements
    Write-OK "Docker Desktop installed."
    Write-Warn "Docker Desktop requires a system RESTART before first use."
    Read-Host "  Press Enter to exit - restart, then re-run this script"
    exit 0
}

if (-not $dockerRunning) {
    Write-Warn "Attempting to start Docker Desktop..."
    $dockerExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerExe) {
        Start-Process $dockerExe
    }
    Write-Host "  Waiting up to 90 seconds for Docker to start..." -ForegroundColor White
    $waited = 0
    while (-not $dockerRunning -and $waited -lt 90) {
        Start-Sleep -Seconds 5
        $waited += 5
        $prev = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        docker info 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { $dockerRunning = $true }
        $ErrorActionPreference = $prev
    }

    if ($dockerRunning) {
        Write-OK "Docker Desktop is now running."
    } else {
        Write-Fail "Docker did not start within 90 seconds."
        Write-Host "  Start Docker Desktop manually and re-run this script." -ForegroundColor White
        exit 1
    }
}

# -- Docker Compose v2 ---------------------------------
Write-Step "Checking Docker Compose v2..."
$composeOut = docker compose version 2>&1 | Out-String
if ($composeOut -match "v(\d+\.\d+\.\d+)") {
    Write-OK "Docker Compose v$($Matches[1])"
} else {
    Write-Fail "Docker Compose v2 not available. Update Docker Desktop to 4.x or later."
    exit 1
}

# -- Docker memory check -------------------------------
Write-Step "Checking Docker memory allocation..."
$memRaw = docker info --format "{{.MemTotal}}" 2>&1 | Out-String
$memRaw = $memRaw.Trim()
if ($memRaw -match "^\d+$") {
    $memGB = [math]::Round([long]$memRaw / 1GB, 1)
    if ($memGB -lt 6) {
        Write-Warn "Docker has only ${memGB} GB allocated. Recommend at least 8 GB."
        Write-Host "  Adjust in Docker Desktop: Settings - Resources - Memory" -ForegroundColor White
    } else {
        Write-OK "Docker memory: ${memGB} GB"
    }
} else {
    Write-Warn "Could not read Docker memory info - skipping check."
}

# -- Python 3.10+ --------------------------------------
Write-Step "Checking Python 3.10 or later..."
$pythonCmd = $null
foreach ($cmd in @("python", "python3", "py")) {
    if (Test-CommandExists $cmd) {
        $ver = & $cmd --version 2>&1 | Out-String
        if ($ver -match "Python 3\.(\d+)" -and [int]$Matches[1] -ge 10) {
            $pythonCmd = $cmd
            Write-Skip $ver.Trim()
            break
        }
    }
}

if (-not $pythonCmd) {
    Write-Warn "Python 3.10 or later not found. Installing Python 3.12 via winget..."
    winget install -e --id Python.Python.3.12 --accept-source-agreements --accept-package-agreements
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("PATH", "User")
    $pythonCmd = "python"
    Write-OK "Python installed: $(& python --version 2>&1)"
}


# ======================================================
# PHASE 2: Project Configuration
# ======================================================
Write-Header "Phase 2/4 - Project Configuration"

# Move to project root (this script lives in scripts/)
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot
Write-Host "  Project root: $ProjectRoot" -ForegroundColor DarkGray

# -- .env file -----------------------------------------
Write-Step "Setting up .env file..."
if (Test-Path ".env") {
    Write-Skip ".env already exists"
} else {
    Copy-Item ".env.example" ".env"
    Write-OK ".env created from .env.example"
}

$envContent = Get-Content ".env" -Raw
if ($envContent -match "<your_n2yo_api_key>") {
    Write-Warn "N2YO_API_KEY is not set yet."
    Write-Host "    Register at https://www.n2yo.com/register/" -ForegroundColor White
    Write-Host "    Then open .env and replace <your_n2yo_api_key> with your key." -ForegroundColor White
}
if ($envContent -match "NASA_API_KEY=DEMO_KEY") {
    Write-Warn "NASA_API_KEY is using DEMO_KEY (30 req/hour limit)."
    Write-Host "    Get a free personal key at https://api.nasa.gov/ for 1000 req/hour." -ForegroundColor White
}

# -- Python virtual environment ------------------------
Write-Step "Setting up Python virtual environment..."
if (Test-Path ".venv\Scripts\Activate.ps1") {
    Write-Skip ".venv already exists"
} else {
    & $pythonCmd -m venv .venv
    Write-OK "Virtual environment created at .venv"
}

# -- Python packages -----------------------------------
Write-Step "Installing Python dependencies..."
$pip = Join-Path $ProjectRoot ".venv\Scripts\pip.exe"
& $pip install --upgrade pip --quiet
& $pip install -r requirements.txt
Write-OK "Python packages installed"


# ======================================================
# PHASE 3: Docker Image Pre-pull
# ======================================================
Write-Header "Phase 3/4 - Pre-pulling Docker Images"

if ($SkipDockerPull) {
    Write-Warn "Skipping image pull (-SkipDockerPull flag set)."
    Write-Host "  First run of start.ps1 will pull images on demand (slower)." -ForegroundColor DarkGray
} else {
    Write-Host "  Pulling all images now so the first startup is fast." -ForegroundColor DarkGray
    Write-Host "  Downloads approx 3-4 GB. May take 5-15 min on first run." -ForegroundColor DarkGray
    Write-Host ""

    $images = @(
        "confluentinc/cp-zookeeper:7.5.3",
        "confluentinc/cp-kafka:7.5.3",
        "provectuslabs/kafka-ui:latest",
        "bitnami/spark:3.5.1",
        "redis:7.2-alpine",
        "postgres:15-alpine",
        "apache/airflow:2.8.1",
        "grafana/grafana:10.2.3"
    )

    foreach ($image in $images) {
        Write-Host "  Pulling $image ..." -ForegroundColor DarkGray -NoNewline
        docker pull $image --quiet
        Write-Host " done" -ForegroundColor Green
    }

    Write-Step "Building custom Hadoop image..."
    docker compose -f "docker\docker-compose.yml" build namenode datanode
    Write-OK "Hadoop image built"
}


# ======================================================
# PHASE 4: Summary
# ======================================================
Write-Header "Phase 4/4 - Setup Complete"

Write-Host ""
Write-Host "  Everything is ready. Next steps:" -ForegroundColor White
Write-Host ""
Write-Host "  1. Fill in your API keys in .env:" -ForegroundColor White
Write-Host "       N2YO  -> https://www.n2yo.com/register/" -ForegroundColor DarkGray
Write-Host "       NASA  -> https://api.nasa.gov/" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  2. Start the platform:" -ForegroundColor White
Write-Host "       .\scripts\start.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  3. Verify all services are up:" -ForegroundColor White
Write-Host "       .\scripts\health-check.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  4. Activate Python venv before running producers:" -ForegroundColor White
Write-Host "       .venv\Scripts\Activate.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "======================================================"
