export function formatNumber(value: number, decimals = 0): string {
  return new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

export function formatCompact(value: number, decimals = 0): string {
  return new Intl.NumberFormat("es-ES", {
    notation: "compact",
    compactDisplay: "short",
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

export function formatPercent(value: number): string {
  return new Intl.NumberFormat("es-ES", {
    style: "percent",
    minimumFractionDigits: 0,
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatDateLabel(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("es-ES", {
    day: "2-digit",
    month: "short",
  }).format(date);
}

export function formatWeekLabel(startDate: string, weekIso: number): string {
  const date = new Date(startDate);
  if (Number.isNaN(date.getTime())) {
    return `W${weekIso}`;
  }
  const month = new Intl.DateTimeFormat("es-ES", { month: "short" }).format(date);
  return `W${weekIso} | ${month}`;
}

export function parseDate(value: string): number {
  const date = new Date(value);
  return date.getTime();
}
