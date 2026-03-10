#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
WEB_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$WEB_ROOT/.." && pwd)"
SOURCE_DIR="$PROJECT_ROOT/outputs"
TARGET_DIR="$WEB_ROOT/public/data"

mkdir -p "$TARGET_DIR"

for file in forecast_weekly_business.csv forecast_daily_business.csv backtest_metrics.csv; do
  if [ -f "$SOURCE_DIR/$file" ]; then
    cp "$SOURCE_DIR/$file" "$TARGET_DIR/$file"
    echo "Copiado $file"
  else
    echo "No existe $file en outputs, se mantiene fallback mock"
  fi
done
