import type {
  BacktestMetricRow,
  ChartPoint,
  DashboardData,
  DailyBusinessRow,
  Focus,
  Frequency,
  KpiCardModel,
  Quantile,
  RangePreset,
  WeeklyBusinessRow,
} from "../types";
import { parseCsv, toCsv } from "./csv";
import { mockBacktestMetrics, mockDailyBusiness, mockWeeklyBusiness } from "./mocks";
import { formatDateLabel, formatWeekLabel, parseDate } from "./format";

function dailyValue(
  row: DailyBusinessRow,
  metric: "eventos_entrega" | "m3_out" | "pales_out" | "cajas_out" | "eventos_recogida" | "m3_in" | "pales_in" | "cajas_in" | "picking_movs_esperados",
  quantile: Quantile,
): number {
  return Number(row[`${metric}_${quantile}` as keyof DailyBusinessRow] ?? 0);
}

function weeklyValue(
  row: WeeklyBusinessRow,
  metric: "eventos_entrega_semana" | "m3_out_semana" | "pales_out_semana" | "cajas_out_semana" | "eventos_recogida_semana" | "m3_in_semana" | "pales_in_semana" | "cajas_in_semana" | "picking_movs_esperados_semana",
  quantile: Quantile,
): number {
  return Number(row[`${metric}_${quantile}` as keyof WeeklyBusinessRow] ?? 0);
}

async function fetchCsv<T extends Record<string, unknown>>(
  url: string,
): Promise<{ rows: T[]; source: "csv" | "mock" | "missing" }> {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return { rows: [], source: "missing" };
    }
    const text = await response.text();
    return { rows: parseCsv<T>(text), source: "csv" };
  } catch {
    return { rows: [], source: "missing" };
  }
}

function coerceDaily(rows: Record<string, unknown>[]): DailyBusinessRow[] {
  return rows.map((row) => ({
    fecha: String(row.fecha ?? ""),
    eventos_entrega_p50: Number(row.eventos_entrega_p50 ?? 0),
    eventos_entrega_p80: Number(row.eventos_entrega_p80 ?? 0),
    m3_out_p50: Number(row.m3_out_p50 ?? 0),
    m3_out_p80: Number(row.m3_out_p80 ?? 0),
    pales_out_p50: Number(row.pales_out_p50 ?? 0),
    pales_out_p80: Number(row.pales_out_p80 ?? 0),
    cajas_out_p50: Number(row.cajas_out_p50 ?? 0),
    cajas_out_p80: Number(row.cajas_out_p80 ?? 0),
    peso_facturable_out_p50: Number(row.peso_facturable_out_p50 ?? 0),
    peso_facturable_out_p80: Number(row.peso_facturable_out_p80 ?? 0),
    eventos_recogida_p50: Number(row.eventos_recogida_p50 ?? 0),
    eventos_recogida_p80: Number(row.eventos_recogida_p80 ?? 0),
    m3_in_p50: Number(row.m3_in_p50 ?? 0),
    m3_in_p80: Number(row.m3_in_p80 ?? 0),
    pales_in_p50: Number(row.pales_in_p50 ?? 0),
    pales_in_p80: Number(row.pales_in_p80 ?? 0),
    cajas_in_p50: Number(row.cajas_in_p50 ?? 0),
    cajas_in_p80: Number(row.cajas_in_p80 ?? 0),
    peso_facturable_in_p50: Number(row.peso_facturable_in_p50 ?? 0),
    peso_facturable_in_p80: Number(row.peso_facturable_in_p80 ?? 0),
    picking_movs_esperados_p50: Number(row.picking_movs_esperados_p50 ?? 0),
    picking_movs_esperados_p80: Number(row.picking_movs_esperados_p80 ?? 0),
  }));
}

function coerceWeekly(rows: Record<string, unknown>[]): WeeklyBusinessRow[] {
  return rows.map((row) => ({
    week_iso: Number(row.week_iso ?? 0),
    year: Number(row.year ?? 0),
    week_start_date: String(row.week_start_date ?? ""),
    week_end_date: String(row.week_end_date ?? ""),
    eventos_entrega_semana_p50: Number(row.eventos_entrega_semana_p50 ?? 0),
    eventos_entrega_semana_p80: Number(row.eventos_entrega_semana_p80 ?? 0),
    m3_out_semana_p50: Number(row.m3_out_semana_p50 ?? 0),
    m3_out_semana_p80: Number(row.m3_out_semana_p80 ?? 0),
    pales_out_semana_p50: Number(row.pales_out_semana_p50 ?? 0),
    pales_out_semana_p80: Number(row.pales_out_semana_p80 ?? 0),
    cajas_out_semana_p50: Number(row.cajas_out_semana_p50 ?? 0),
    cajas_out_semana_p80: Number(row.cajas_out_semana_p80 ?? 0),
    peso_facturable_out_semana_p50: Number(row.peso_facturable_out_semana_p50 ?? 0),
    peso_facturable_out_semana_p80: Number(row.peso_facturable_out_semana_p80 ?? 0),
    eventos_recogida_semana_p50: Number(row.eventos_recogida_semana_p50 ?? 0),
    eventos_recogida_semana_p80: Number(row.eventos_recogida_semana_p80 ?? 0),
    m3_in_semana_p50: Number(row.m3_in_semana_p50 ?? 0),
    m3_in_semana_p80: Number(row.m3_in_semana_p80 ?? 0),
    pales_in_semana_p50: Number(row.pales_in_semana_p50 ?? 0),
    pales_in_semana_p80: Number(row.pales_in_semana_p80 ?? 0),
    cajas_in_semana_p50: Number(row.cajas_in_semana_p50 ?? 0),
    cajas_in_semana_p80: Number(row.cajas_in_semana_p80 ?? 0),
    peso_facturable_in_semana_p50: Number(row.peso_facturable_in_semana_p50 ?? 0),
    peso_facturable_in_semana_p80: Number(row.peso_facturable_in_semana_p80 ?? 0),
    picking_movs_esperados_semana_p50: Number(row.picking_movs_esperados_semana_p50 ?? 0),
    picking_movs_esperados_semana_p80: Number(row.picking_movs_esperados_semana_p80 ?? 0),
    picking_movs_reales_semana: Number(row.picking_movs_reales_semana ?? 0),
    picking_movs_no_atribuibles_semana: Number(row.picking_movs_no_atribuibles_semana ?? 0),
    picking_movs_no_atribuibles_semana_p50: Number(row.picking_movs_no_atribuibles_semana_p50 ?? 0),
    picking_movs_no_atribuibles_semana_p80: Number(row.picking_movs_no_atribuibles_semana_p80 ?? 0),
  }));
}

function coerceBacktest(rows: Record<string, unknown>[]): BacktestMetricRow[] {
  return rows.map((row) => ({
    axis: String(row.axis ?? ""),
    freq: String(row.freq ?? ""),
    target: String(row.target ?? ""),
    model: String(row.model ?? ""),
    fold_id: Number(row.fold_id ?? 0),
    fold_start: String(row.fold_start ?? ""),
    fold_end: String(row.fold_end ?? ""),
    wape: Number(row.wape ?? 0),
    smape: Number(row.smape ?? 0),
    mase: Number(row.mase ?? 0),
    wape_peak5: Number(row.wape_peak5 ?? 0),
    coverage_empirical: row.coverage_empirical == null ? undefined : Number(row.coverage_empirical),
  }));
}

export async function loadDashboardData(): Promise<DashboardData> {
  const [dailyResult, weeklyResult, backtestResult] = await Promise.all([
    fetchCsv<Record<string, unknown>>("/data/forecast_daily_business.csv"),
    fetchCsv<Record<string, unknown>>("/data/forecast_weekly_business.csv"),
    fetchCsv<Record<string, unknown>>("/data/backtest_metrics.csv"),
  ]);

  return {
    daily: dailyResult.rows.length > 0 ? coerceDaily(dailyResult.rows) : mockDailyBusiness,
    weekly: weeklyResult.rows.length > 0 ? coerceWeekly(weeklyResult.rows) : mockWeeklyBusiness,
    backtest: backtestResult.rows.length > 0 ? coerceBacktest(backtestResult.rows) : mockBacktestMetrics,
    sources: {
      daily: dailyResult.rows.length > 0 ? "csv" : "mock",
      weekly: weeklyResult.rows.length > 0 ? "csv" : "mock",
      backtest: backtestResult.rows.length > 0 ? "csv" : "mock",
    },
  };
}

export function applyRange<T extends DailyBusinessRow | WeeklyBusinessRow>(
  rows: T[],
  rangePreset: RangePreset,
  frequency: Frequency,
): T[] {
  if (rangePreset === "all" || rows.length === 0) {
    return rows;
  }

  const lastRow = rows[rows.length - 1];
  const lastDate =
    frequency === "daily"
      ? parseDate((lastRow as DailyBusinessRow).fecha)
      : parseDate((lastRow as WeeklyBusinessRow).week_start_date);

  const days = { "30d": 30, "90d": 90, "180d": 180, "365d": 365 }[rangePreset];
  const threshold = lastDate - days * 24 * 60 * 60 * 1000;

  return rows.filter((row) => {
    const rowDate =
      frequency === "daily"
        ? parseDate((row as DailyBusinessRow).fecha)
        : parseDate((row as WeeklyBusinessRow).week_start_date);
    return rowDate >= threshold;
  });
}

export function buildChartPoints(
  dailyRows: DailyBusinessRow[],
  weeklyRows: WeeklyBusinessRow[],
  frequency: Frequency,
  quantile: Quantile,
): ChartPoint[] {
  if (frequency === "daily") {
    return dailyRows.map((row) => ({
      label: formatDateLabel(row.fecha),
      dateKey: row.fecha,
      outEvents: dailyValue(row, "eventos_entrega", quantile),
      outM3: dailyValue(row, "m3_out", quantile),
      outPales: dailyValue(row, "pales_out", quantile),
      outCajas: dailyValue(row, "cajas_out", quantile),
      inEvents: dailyValue(row, "eventos_recogida", quantile),
      inM3: dailyValue(row, "m3_in", quantile),
      inPales: dailyValue(row, "pales_in", quantile),
      inCajas: dailyValue(row, "cajas_in", quantile),
      picking: dailyValue(row, "picking_movs_esperados", quantile),
    }));
  }

  return weeklyRows.map((row) => ({
    label: formatWeekLabel(row.week_start_date, row.week_iso),
    dateKey: row.week_start_date,
    outEvents: weeklyValue(row, "eventos_entrega_semana", quantile),
    outM3: weeklyValue(row, "m3_out_semana", quantile),
    outPales: weeklyValue(row, "pales_out_semana", quantile),
    outCajas: weeklyValue(row, "cajas_out_semana", quantile),
    inEvents: weeklyValue(row, "eventos_recogida_semana", quantile),
    inM3: weeklyValue(row, "m3_in_semana", quantile),
    inPales: weeklyValue(row, "pales_in_semana", quantile),
    inCajas: weeklyValue(row, "cajas_in_semana", quantile),
    picking: weeklyValue(row, "picking_movs_esperados_semana", quantile),
    pickingReal: row.picking_movs_reales_semana,
    pickingNoAtrib:
      quantile === "p50"
        ? row.picking_movs_no_atribuibles_semana_p50 ?? row.picking_movs_no_atribuibles_semana
        : row.picking_movs_no_atribuibles_semana_p80 ?? row.picking_movs_no_atribuibles_semana,
  }));
}

export function buildKpis(chartPoints: ChartPoint[]): KpiCardModel[] {
  const totals = chartPoints.reduce(
    (acc, row) => ({
      outEvents: acc.outEvents + row.outEvents,
      outM3: acc.outM3 + row.outM3,
      outPales: acc.outPales + row.outPales,
      outCajas: acc.outCajas + row.outCajas,
      inEvents: acc.inEvents + row.inEvents,
      inM3: acc.inM3 + row.inM3,
      inPales: acc.inPales + row.inPales,
      inCajas: acc.inCajas + row.inCajas,
      picking: acc.picking + row.picking,
    }),
    { outEvents: 0, outM3: 0, outPales: 0, outCajas: 0, inEvents: 0, inM3: 0, inPales: 0, inCajas: 0, picking: 0 },
  );

  return [
    { id: "out-events", label: "OUT eventos", value: totals.outEvents, tone: "out" },
    { id: "out-m3", label: "OUT m3", value: totals.outM3, tone: "out", decimals: 1 },
    { id: "out-pales", label: "OUT pales", value: totals.outPales, tone: "out" },
    { id: "out-cajas", label: "OUT cajas", value: totals.outCajas, tone: "out" },
    { id: "in-events", label: "IN eventos", value: totals.inEvents, tone: "in" },
    { id: "in-m3", label: "IN m3", value: totals.inM3, tone: "in", decimals: 1 },
    { id: "in-pales", label: "IN pales", value: totals.inPales, tone: "in" },
    { id: "in-cajas", label: "IN cajas", value: totals.inCajas, tone: "in" },
    { id: "picking", label: "Picking esperado", value: totals.picking, tone: "picking" },
  ];
}

export function getBacktestHighlights(backtest: BacktestMetricRow[], frequency: Frequency) {
  const scoped = backtest.filter((row) => row.freq === frequency && row.model.includes("p50"));
  const serviceRows = scoped.filter((row) => row.axis === "service");
  const pickingRows = scoped.filter(
    (row) => row.axis === "workload_expected_from_service" && row.target.includes("picking"),
  );

  const average = (rows: BacktestMetricRow[], field: keyof BacktestMetricRow) =>
    rows.length === 0 ? 0 : rows.reduce((acc, row) => acc + Number(row[field] ?? 0), 0) / rows.length;

  return {
    serviceWape: average(serviceRows, "wape"),
    servicePeakWape: average(serviceRows, "wape_peak5"),
    pickingWape: average(pickingRows, "wape"),
    pickingCoverage: average(pickingRows, "coverage_empirical"),
  };
}

export function exportFilteredCsv(chartPoints: ChartPoint[], focus: Focus, filename: string): void {
  const rows = chartPoints.map((row) => {
    const base = { fecha: row.dateKey };
    if (focus === "out") {
      return { ...base, eventos_out: row.outEvents, m3_out: row.outM3, pales_out: row.outPales, cajas_out: row.outCajas };
    }
    if (focus === "in") {
      return { ...base, eventos_in: row.inEvents, m3_in: row.inM3, pales_in: row.inPales, cajas_in: row.inCajas };
    }
    if (focus === "picking") {
      return { ...base, picking_esperado: row.picking, picking_real: row.pickingReal ?? "", picking_no_atribuible: row.pickingNoAtrib ?? "" };
    }
    return {
      ...base,
      eventos_out: row.outEvents,
      m3_out: row.outM3,
      pales_out: row.outPales,
      cajas_out: row.outCajas,
      eventos_in: row.inEvents,
      m3_in: row.inM3,
      pales_in: row.inPales,
      cajas_in: row.inCajas,
      picking_esperado: row.picking,
    };
  });

  const blob = new Blob([toCsv(rows)], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}
