export type Quantile = "p50" | "p80";
export type Frequency = "daily" | "weekly";
export type Focus = "all" | "out" | "in" | "picking";
export type RangePreset = "30d" | "90d" | "180d" | "365d" | "all";

export type DailyBusinessRow = {
  fecha: string;
  eventos_entrega_p50: number;
  eventos_entrega_p80: number;
  m3_out_p50: number;
  m3_out_p80: number;
  pales_out_p50: number;
  pales_out_p80: number;
  cajas_out_p50: number;
  cajas_out_p80: number;
  peso_facturable_out_p50: number;
  peso_facturable_out_p80: number;
  eventos_recogida_p50: number;
  eventos_recogida_p80: number;
  m3_in_p50: number;
  m3_in_p80: number;
  pales_in_p50: number;
  pales_in_p80: number;
  cajas_in_p50: number;
  cajas_in_p80: number;
  peso_facturable_in_p50: number;
  peso_facturable_in_p80: number;
  picking_movs_esperados_p50: number;
  picking_movs_esperados_p80: number;
};

export type WeeklyBusinessRow = {
  week_iso: number;
  year: number;
  week_start_date: string;
  week_end_date: string;
  eventos_entrega_semana_p50: number;
  eventos_entrega_semana_p80: number;
  m3_out_semana_p50: number;
  m3_out_semana_p80: number;
  pales_out_semana_p50: number;
  pales_out_semana_p80: number;
  cajas_out_semana_p50: number;
  cajas_out_semana_p80: number;
  peso_facturable_out_semana_p50: number;
  peso_facturable_out_semana_p80: number;
  eventos_recogida_semana_p50: number;
  eventos_recogida_semana_p80: number;
  m3_in_semana_p50: number;
  m3_in_semana_p80: number;
  pales_in_semana_p50: number;
  pales_in_semana_p80: number;
  cajas_in_semana_p50: number;
  cajas_in_semana_p80: number;
  peso_facturable_in_semana_p50: number;
  peso_facturable_in_semana_p80: number;
  picking_movs_esperados_semana_p50: number;
  picking_movs_esperados_semana_p80: number;
  picking_movs_reales_semana?: number;
  picking_movs_no_atribuibles_semana?: number;
  picking_movs_no_atribuibles_semana_p50?: number;
  picking_movs_no_atribuibles_semana_p80?: number;
};

export type BacktestMetricRow = {
  axis: string;
  freq: string;
  target: string;
  model: string;
  fold_id: number;
  fold_start: string;
  fold_end: string;
  wape: number;
  smape?: number;
  mase?: number;
  wape_peak5?: number;
  coverage_empirical?: number;
};

export type DataOrigin = "csv" | "mock" | "missing";

export type DashboardData = {
  daily: DailyBusinessRow[];
  weekly: WeeklyBusinessRow[];
  backtest: BacktestMetricRow[];
  sources: {
    daily: DataOrigin;
    weekly: DataOrigin;
    backtest: DataOrigin;
  };
};

export type ChartPoint = {
  label: string;
  dateKey: string;
  outEvents: number;
  outM3: number;
  outPales: number;
  outCajas: number;
  inEvents: number;
  inM3: number;
  inPales: number;
  inCajas: number;
  picking: number;
  pickingReal?: number;
  pickingNoAtrib?: number;
};

export type KpiCardModel = {
  id: string;
  label: string;
  value: number;
  tone: "out" | "in" | "picking";
  suffix?: string;
  decimals?: number;
};
