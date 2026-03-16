import type {
  SupervisorChartPoint,
  SupervisorDashboardData,
  SupervisorDailyRow,
  SupervisorWeeklyRow,
} from "../types";
import { parseCsv } from "./csv";
import { formatWeekLabel, parseDate } from "./format";
import { mockSupervisorDaily, mockSupervisorWeekly } from "./mocks";

export function weekKey(row: Pick<SupervisorWeeklyRow, "year" | "week_iso">): string {
  return `${row.year}-W${String(row.week_iso).padStart(2, "0")}`;
}

async function fetchCsv<T extends Record<string, unknown>>(url: string): Promise<T[] | null> {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    const text = await response.text();
    return parseCsv<T>(text);
  } catch {
    return null;
  }
}

function toNumber(value: unknown): number {
  return Number(value ?? 0);
}

function coerceDaily(rows: Record<string, unknown>[]): SupervisorDailyRow[] {
  return rows
    .map((row) => ({
      fecha: String(row.fecha ?? ""),
      year: toNumber(row.year),
      week_iso: toNumber(row.week_iso),
      week_start_date: String(row.week_start_date ?? ""),
      week_end_date: String(row.week_end_date ?? ""),
      weekday: toNumber(row.weekday),
      salidas_forecast: toNumber(row.salidas_forecast),
      salidas_2024: toNumber(row.salidas_2024),
      salidas_real_2026: toNumber(row.salidas_real_2026),
      recogidas_forecast: toNumber(row.recogidas_forecast),
      recogidas_2024: toNumber(row.recogidas_2024),
      recogidas_real_2026: toNumber(row.recogidas_real_2026),
      pick_lines_forecast: toNumber(row.pick_lines_forecast),
      pick_lines_2024: toNumber(row.pick_lines_2024),
      pick_lines_real_2026: toNumber(row.pick_lines_real_2026),
      cutoff_date: String(row.cutoff_date ?? ""),
      forecast_snapshot_date: String(row.forecast_snapshot_date ?? ""),
      year_target: toNumber(row.year_target || 2026),
      comparison_year: toNumber(row.comparison_year || 2024),
    }))
    .sort((a, b) => parseDate(a.fecha) - parseDate(b.fecha));
}

function coerceWeekly(rows: Record<string, unknown>[]): SupervisorWeeklyRow[] {
  return rows
    .map((row) => ({
      year: toNumber(row.year),
      week_iso: toNumber(row.week_iso),
      week_start_date: String(row.week_start_date ?? ""),
      week_end_date: String(row.week_end_date ?? ""),
      salidas_forecast: toNumber(row.salidas_forecast),
      salidas_2024: toNumber(row.salidas_2024),
      salidas_real_2026: toNumber(row.salidas_real_2026),
      recogidas_forecast: toNumber(row.recogidas_forecast),
      recogidas_2024: toNumber(row.recogidas_2024),
      recogidas_real_2026: toNumber(row.recogidas_real_2026),
      pick_lines_forecast: toNumber(row.pick_lines_forecast),
      pick_lines_2024: toNumber(row.pick_lines_2024),
      pick_lines_real_2026: toNumber(row.pick_lines_real_2026),
      cutoff_date: String(row.cutoff_date ?? ""),
      forecast_snapshot_date: String(row.forecast_snapshot_date ?? ""),
      year_target: toNumber(row.year_target || 2026),
      comparison_year: toNumber(row.comparison_year || 2024),
    }))
    .sort((a, b) => parseDate(a.week_start_date) - parseDate(b.week_start_date));
}

function resolveTargetYear(rows: SupervisorWeeklyRow[]): number {
  if (rows.length === 0) {
    return 2026;
  }
  const yearFromData = rows.find((row) => Number.isFinite(row.year_target) && row.year_target > 0);
  return yearFromData?.year_target ?? 2026;
}

export function getTargetYearRows(rows: SupervisorWeeklyRow[]): SupervisorWeeklyRow[] {
  const yearTarget = resolveTargetYear(rows);
  const targetRows = rows.filter((row) => row.year === yearTarget);
  if (targetRows.length > 0) {
    return targetRows;
  }
  return rows;
}

export function resolveCutoffDate(rows: SupervisorWeeklyRow[]): string {
  const values = rows
    .map((r) => r.cutoff_date)
    .filter((v) => v && !Number.isNaN(parseDate(v)))
    .sort((a, b) => parseDate(b) - parseDate(a));
  return values[0] ?? "";
}

export async function loadSupervisorDashboardData(): Promise<SupervisorDashboardData> {
  const [dailyRaw, weeklyRaw] = await Promise.all([
    fetchCsv<Record<string, unknown>>("/data/supervisor_dashboard_daily.csv"),
    fetchCsv<Record<string, unknown>>("/data/supervisor_dashboard_weekly.csv"),
  ]);

  const daily = dailyRaw && dailyRaw.length > 0 ? coerceDaily(dailyRaw) : mockSupervisorDaily;
  const weekly = weeklyRaw && weeklyRaw.length > 0 ? coerceWeekly(weeklyRaw) : mockSupervisorWeekly;

  return {
    daily,
    weekly,
    sourceDaily: dailyRaw && dailyRaw.length > 0 ? "csv" : "mock",
    sourceWeekly: weeklyRaw && weeklyRaw.length > 0 ? "csv" : "mock",
  };
}

export function getDefaultActiveWeek(rows: SupervisorWeeklyRow[]): SupervisorWeeklyRow | null {
  const targetRows = getTargetYearRows(rows);
  if (targetRows.length === 0) {
    return null;
  }
  const cutoffDate = resolveCutoffDate(targetRows);
  if (!cutoffDate) {
    return targetRows[Math.max(targetRows.length - 1, 0)] ?? null;
  }
  const cutoffTs = parseDate(cutoffDate);
  const firstFuture = targetRows.find((row) => parseDate(row.week_start_date) >= cutoffTs);
  return firstFuture ?? targetRows[targetRows.length - 1];
}

export function getDefaultRange(rows: SupervisorWeeklyRow[], horizonWeeks: number): {
  startDate: string;
  endDate: string;
  activeWeekKey: string;
} {
  const targetRows = getTargetYearRows(rows);
  if (targetRows.length === 0) {
    return { startDate: "", endDate: "", activeWeekKey: "" };
  }
  const active = getDefaultActiveWeek(targetRows) ?? targetRows[targetRows.length - 1];
  const activeIdx = targetRows.findIndex((row) => weekKey(row) === weekKey(active));
  const startIdx = Math.max(0, activeIdx - 2);
  const endIdx = Math.min(targetRows.length - 1, startIdx + Math.max(horizonWeeks - 1, 0));
  return {
    startDate: targetRows[startIdx]?.week_start_date ?? "",
    endDate: targetRows[endIdx]?.week_start_date ?? "",
    activeWeekKey: weekKey(active),
  };
}

export function filterWeeklyByRange(
  rows: SupervisorWeeklyRow[],
  startDate: string,
  endDate: string,
): SupervisorWeeklyRow[] {
  const targetRows = getTargetYearRows(rows);
  if (!startDate || !endDate) {
    return targetRows;
  }
  const startTs = parseDate(startDate);
  const endTs = parseDate(endDate);
  const minTs = Math.min(startTs, endTs);
  const maxTs = Math.max(startTs, endTs);
  return targetRows.filter((row) => {
    const rowTs = parseDate(row.week_start_date);
    return rowTs >= minTs && rowTs <= maxTs;
  });
}

export function buildSupervisorChartPoints(
  rows: SupervisorWeeklyRow[],
  startDate: string,
  endDate: string,
): SupervisorChartPoint[] {
  const scoped = filterWeeklyByRange(rows, startDate, endDate);
  return scoped.map((row) => ({
    label: formatWeekLabel(row.week_start_date, row.week_iso),
    weekKey: weekKey(row),
    week_start_date: row.week_start_date,
    week_end_date: row.week_end_date,
    salidasForecast: row.salidas_forecast,
    salidas2024: row.salidas_2024,
    salidasReal2026: row.salidas_real_2026,
    recogidasForecast: row.recogidas_forecast,
    recogidas2024: row.recogidas_2024,
    recogidasReal2026: row.recogidas_real_2026,
    pickingForecast: row.pick_lines_forecast,
    picking2024: row.pick_lines_2024,
    pickingReal2026: row.pick_lines_real_2026,
  }));
}
