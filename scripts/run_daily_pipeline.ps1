param(
    [switch]$RefreshCleanInputs,
    [switch]$SkipForecast,
    [switch]$SkipAbc,
    [switch]$SkipBasket,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    $PythonExe = "python"
}

$ArgsList = @(
    (Join-Path $RepoRoot "run_daily_pipeline.py")
)

if ($RefreshCleanInputs) { $ArgsList += "--refresh_clean_inputs" }
if ($SkipForecast) { $ArgsList += "--skip_forecast" }
if ($SkipAbc) { $ArgsList += "--skip_abc" }
if ($SkipBasket) { $ArgsList += "--skip_basket" }
if ($DryRun) { $ArgsList += "--dry_run" }

Push-Location $RepoRoot
try {
    & $PythonExe @ArgsList
}
finally {
    Pop-Location
}
