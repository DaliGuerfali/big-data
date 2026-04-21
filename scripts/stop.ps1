# ─────────────────────────────────────────────────────────
# Platform Stop Script
# Usage: .\scripts\stop.ps1          — stops containers, keeps volumes
#        .\scripts\stop.ps1 -Clean   — stops and removes volumes
# ─────────────────────────────────────────────────────────

param(
    [switch]$Clean
)

$ComposeFile = "docker\docker-compose.yml"

if ($Clean) {
    Write-Host "Stopping all services and removing volumes..."
    docker compose -f $ComposeFile down -v --remove-orphans
    Write-Host "OK All containers and volumes removed."
} else {
    Write-Host "Stopping all services (volumes preserved)..."
    docker compose -f $ComposeFile down
    Write-Host "OK All containers stopped. Data volumes preserved."
    Write-Host "   Use -Clean to also remove volumes."
}
