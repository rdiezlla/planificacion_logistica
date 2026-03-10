$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$webRoot = Split-Path -Parent $scriptDir
$projectRoot = Split-Path -Parent $webRoot
$sourceDir = Join-Path $projectRoot "outputs"
$targetDir = Join-Path $webRoot "public\\data"

New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

$files = @(
  "forecast_weekly_business.csv",
  "forecast_daily_business.csv",
  "backtest_metrics.csv"
)

foreach ($file in $files) {
  $sourceFile = Join-Path $sourceDir $file
  $targetFile = Join-Path $targetDir $file

  if (Test-Path $sourceFile) {
    Copy-Item -Path $sourceFile -Destination $targetFile -Force
    Write-Host "Copiado $file"
  }
  else {
    Write-Host "No existe $file en outputs, se mantiene fallback mock"
  }
}
