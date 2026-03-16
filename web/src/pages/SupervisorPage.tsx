import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Layout } from "../components/Layout";
import { formatNumber, formatPercent, parseDate } from "../lib/format";
import {
  buildSupervisorChartPoints,
  filterWeeklyByRange,
  getDefaultRange,
  getTargetYearRows,
  loadSupervisorDashboardData,
  resolveCutoffDate,
  weekKey,
} from "../lib/supervisor";
import type { SupervisorWeeklyRow } from "../types";

function deltaPercent(current: number, baseline: number): number {
  if (!Number.isFinite(baseline) || baseline <= 0) {
    return 0;
  }
  return (current - baseline) / baseline;
}

function MetricChart({
  title,
  subtitle,
  data,
  forecastKey,
  year2024Key,
  real2026Key,
  forecastColor,
}: {
  title: string;
  subtitle: string;
  data: Array<Record<string, string | number>>;
  forecastKey: string;
  year2024Key: string;
  real2026Key: string;
  forecastColor: string;
}) {
  return (
    <div className="rounded-[20px] border border-white/65 bg-white/92 p-3 shadow-card">
      <div className="mb-2">
        <p className="text-sm font-semibold text-ink">{title}</p>
        <p className="text-xs text-smoke">{subtitle}</p>
      </div>
      <div style={{ width: "100%", height: 170 }}>
        <ResponsiveContainer>
          <LineChart data={data}>
            <CartesianGrid stroke="#ebe4db" strokeDasharray="2 5" vertical={false} />
            <XAxis dataKey="label" tickLine={false} axisLine={false} minTickGap={10} tick={{ fill: "#6b7280", fontSize: 11 }} />
            <YAxis tickLine={false} axisLine={false} width={36} tick={{ fill: "#6b7280", fontSize: 11 }} />
            <Tooltip
              contentStyle={{
                borderRadius: 14,
                border: "1px solid #e7dfd5",
                background: "rgba(255,255,255,0.98)",
                boxShadow: "0 10px 26px rgba(17,24,39,0.11)",
              }}
              formatter={(value: number) => formatNumber(Number(value), 0)}
            />
            <Legend iconType="line" wrapperStyle={{ fontSize: 11 }} />
            <Line type="monotone" dataKey={forecastKey} name="Forecast 2026" stroke={forecastColor} strokeWidth={2.8} dot={false} activeDot={{ r: 4 }} />
            <Line type="monotone" dataKey={year2024Key} name="Año 2024" stroke="#111827" strokeDasharray="5 4" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey={real2026Key} name="Real 2026" stroke="#2d8f68" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export function SupervisorPage() {
  const [weekly, setWeekly] = useState<SupervisorWeeklyRow[]>([]);
  const [sourceWeekly, setSourceWeekly] = useState<"csv" | "mock">("mock");
  const [rangeStartDate, setRangeStartDate] = useState("");
  const [rangeEndDate, setRangeEndDate] = useState("");
  const [activeWeekKey, setActiveWeekKey] = useState("");

  useEffect(() => {
    let mounted = true;
    loadSupervisorDashboardData().then((loaded) => {
      if (!mounted) {
        return;
      }
      setWeekly(loaded.weekly);
      setSourceWeekly(loaded.sourceWeekly);
      const defaults = getDefaultRange(loaded.weekly, 12);
      setRangeStartDate(defaults.startDate);
      setRangeEndDate(defaults.endDate);
      setActiveWeekKey(defaults.activeWeekKey);
    });
    return () => {
      mounted = false;
    };
  }, []);

  const targetYearWeeks = useMemo(() => getTargetYearRows(weekly), [weekly]);
  const scopedWeeks = useMemo(
    () => filterWeeklyByRange(targetYearWeeks, rangeStartDate, rangeEndDate),
    [targetYearWeeks, rangeStartDate, rangeEndDate],
  );
  const chartPoints = useMemo(
    () => buildSupervisorChartPoints(targetYearWeeks, rangeStartDate, rangeEndDate),
    [targetYearWeeks, rangeStartDate, rangeEndDate],
  );

  const activeWeek = useMemo(() => {
    const inScope = scopedWeeks.find((row) => weekKey(row) === activeWeekKey);
    if (inScope) {
      return inScope;
    }
    return scopedWeeks[0] ?? targetYearWeeks[0] ?? null;
  }, [scopedWeeks, targetYearWeeks, activeWeekKey]);

  const cutoffDate = resolveCutoffDate(targetYearWeeks);
  const cutoffTs = cutoffDate ? parseDate(cutoffDate) : Number.NEGATIVE_INFINITY;
  const weekHasRealObservation = activeWeek ? parseDate(activeWeek.week_start_date) <= cutoffTs : false;

  const kpis = [
    {
      id: "salidas",
      title: "Numero de salidas forecast",
      forecast: activeWeek?.salidas_forecast ?? 0,
      comparable2024: activeWeek?.salidas_2024 ?? 0,
      real2026: activeWeek?.salidas_real_2026 ?? 0,
      toneClass: "text-coral",
      bgClass: "from-coral/20 to-coral/5",
    },
    {
      id: "recogidas",
      title: "Numero de recogidas forecast",
      forecast: activeWeek?.recogidas_forecast ?? 0,
      comparable2024: activeWeek?.recogidas_2024 ?? 0,
      real2026: activeWeek?.recogidas_real_2026 ?? 0,
      toneClass: "text-sky",
      bgClass: "from-sky/20 to-sky/5",
    },
    {
      id: "picking",
      title: "Numero de lineas a preparar forecast",
      forecast: activeWeek?.pick_lines_forecast ?? 0,
      comparable2024: activeWeek?.pick_lines_2024 ?? 0,
      real2026: activeWeek?.pick_lines_real_2026 ?? 0,
      toneClass: "text-emerald-700",
      bgClass: "from-mint/20 to-mint/5",
    },
  ];

  return (
    <Layout title="Resumen Supervisor" subtitle="" showHeader={false} compact>
      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_260px]">
        <section className="space-y-3">
          <div className="flex items-center justify-between rounded-[18px] border border-white/65 bg-white/86 px-4 py-3 shadow-card">
            <div>
              <p className="text-[11px] uppercase tracking-[0.2em] text-smoke">Supervisor summary</p>
              <h1 className="mt-1 text-xl font-semibold text-ink">Forecast vs 2024 vs Real 2026</h1>
            </div>
            <div className="rounded-full bg-fog px-3 py-2 text-[11px] uppercase tracking-[0.18em] text-smoke">
              fuente {sourceWeekly}
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            {kpis.map((kpi) => {
              const deltaVs2024 = deltaPercent(kpi.forecast, kpi.comparable2024);
              const deltaRealVs2024 = deltaPercent(kpi.real2026, kpi.comparable2024);
              return (
                <div key={kpi.id} className="rounded-[18px] border border-white/70 bg-white/92 p-3 shadow-card">
                  <div className={`inline-flex rounded-full bg-gradient-to-r px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${kpi.bgClass} ${kpi.toneClass}`}>
                    {kpi.title}
                  </div>
                  <p className="mt-3 text-3xl font-semibold text-ink">{formatNumber(kpi.forecast, 0)}</p>
                  <p className="mt-1 text-xs text-smoke">
                    Forecast vs 2024: <span className="font-semibold text-ink">{formatPercent(deltaVs2024)}</span> | 2024:{" "}
                    <span className="font-semibold text-ink">{formatNumber(kpi.comparable2024, 0)}</span>
                  </p>
                  <p className="mt-1 text-xs text-smoke">
                    {weekHasRealObservation
                      ? `Real 2026 observado: ${formatNumber(kpi.real2026, 0)} (vs 2024: ${formatPercent(deltaRealVs2024)})`
                      : "Real 2026 observado: pendiente para semana futura"}
                  </p>
                </div>
              );
            })}
          </div>

          <div className="space-y-2">
            <MetricChart
              title="Numero de salidas"
              subtitle="Comparativa semanal simplificada."
              data={chartPoints}
              forecastKey="salidasForecast"
              year2024Key="salidas2024"
              real2026Key="salidasReal2026"
              forecastColor="#e2568c"
            />
            <MetricChart
              title="Numero de recogidas"
              subtitle="Forecast principal vs referencias historicas."
              data={chartPoints}
              forecastKey="recogidasForecast"
              year2024Key="recogidas2024"
              real2026Key="recogidasReal2026"
              forecastColor="#5f90ff"
            />
            <MetricChart
              title="Numero de lineas a preparar"
              subtitle="Picking esperado (solo salidas) frente a 2024 y real 2026."
              data={chartPoints}
              forecastKey="pickingForecast"
              year2024Key="picking2024"
              real2026Key="pickingReal2026"
              forecastColor="#2d8f68"
            />
          </div>
        </section>

        <aside className="rounded-[20px] border border-white/70 bg-white/92 p-3 shadow-card">
          <p className="text-sm font-semibold text-ink">Calendario de semanas</p>
          <p className="mt-1 text-xs text-smoke">Selecciona semana o rango de dias para filtrar KPIs y graficos.</p>

          <div className="mt-3 grid gap-2">
            <label className="text-[11px] uppercase tracking-[0.16em] text-smoke">
              Inicio rango
              <input
                type="date"
                value={rangeStartDate}
                onChange={(event) => setRangeStartDate(event.target.value)}
                className="mt-1 w-full rounded-xl border border-line bg-white px-2 py-2 text-xs text-ink"
              />
            </label>
            <label className="text-[11px] uppercase tracking-[0.16em] text-smoke">
              Fin rango
              <input
                type="date"
                value={rangeEndDate}
                onChange={(event) => setRangeEndDate(event.target.value)}
                className="mt-1 w-full rounded-xl border border-line bg-white px-2 py-2 text-xs text-ink"
              />
            </label>
          </div>

          <button
            type="button"
            onClick={() => {
              if (!activeWeek) {
                return;
              }
              setRangeStartDate(activeWeek.week_start_date);
              setRangeEndDate(activeWeek.week_start_date);
            }}
            className="mt-3 w-full rounded-xl border border-ink bg-ink px-3 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-white"
          >
            Usar solo semana activa
          </button>

          <div className="mt-3 max-h-[420px] space-y-1 overflow-auto pr-1">
            {targetYearWeeks.map((row) => {
              const key = weekKey(row);
              const isActive = key === (activeWeek ? weekKey(activeWeek) : "");
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => setActiveWeekKey(key)}
                  className={[
                    "w-full rounded-xl border px-2 py-2 text-left transition",
                    isActive ? "border-ink bg-ink text-white" : "border-line bg-fog/35 text-ink hover:bg-fog/70",
                  ].join(" ")}
                >
                  <div className="text-[11px] uppercase tracking-[0.16em] opacity-80">
                    WK{row.week_iso} | {row.week_start_date.slice(5)} - {row.week_end_date.slice(5)}
                  </div>
                  <div className="mt-1 grid grid-cols-3 gap-1 text-[11px]">
                    <span>F:{formatNumber(row.salidas_forecast, 0)}</span>
                    <span>24:{formatNumber(row.salidas_2024, 0)}</span>
                    <span>R:{formatNumber(row.salidas_real_2026, 0)}</span>
                  </div>
                </button>
              );
            })}
          </div>
        </aside>
      </div>
    </Layout>
  );
}
