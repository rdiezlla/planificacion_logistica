$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$webRoot = Split-Path -Parent $scriptDir
$projectRoot = Split-Path -Parent $webRoot
$sourceDir = Join-Path $projectRoot "outputs"
$targetDir = Join-Path $webRoot "dist\\data"

New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

$files = @(
  "forecast_weekly_business.csv",
  "forecast_daily_business.csv",
  "backtest_metrics.csv",
  "supervisor_dashboard_daily.csv",
  "supervisor_dashboard_weekly.csv"
)

foreach ($file in $files) {
  $sourceFile = Join-Path $sourceDir $file
  $targetFile = Join-Path $targetDir $file

  if (Test-Path $sourceFile) {
    Copy-Item -Path $sourceFile -Destination $targetFile -Force
    Write-Host "Copiado $file a dist/data"
  }
  else {
    Write-Host "No existe $file en outputs, se mantiene el archivo actual en dist/data (si existe)"
  }
}
