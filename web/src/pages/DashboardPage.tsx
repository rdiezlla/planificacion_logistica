import { useEffect, useMemo, useState } from "react";
import { ChartCard } from "../components/ChartCard";
import { DataStatus } from "../components/DataStatus";
import { FilterBar } from "../components/FilterBar";
import { KpiCard } from "../components/KpiCard";
import { Layout } from "../components/Layout";
import { SummaryCard } from "../components/SummaryCard";
import {
  applyRange,
  buildChartPoints,
  buildKpis,
  exportFilteredCsv,
  getBacktestHighlights,
  loadDashboardData,
} from "../lib/data";
import { formatCompact, formatPercent } from "../lib/format";
import type { DashboardData, Focus, Frequency, Quantile, RangePreset } from "../types";

type PageMode = "overview" | "transport" | "warehouse";

const pageConfig: Record<PageMode, { title: string; subtitle: string; defaultFocus: Focus }> = {
  overview: {
    title: "Overview",
    subtitle:
      "Vista unificada de servicios OUT/IN y picking esperado, pensada para seguimiento semanal o diario con carga directa desde CSV.",
    defaultFocus: "all",
  },
  transport: {
    title: "Transport",
    subtitle:
      "Seguimiento de entregas y recogidas por fecha de servicio con KPIs listos para transporte y facturacion.",
    defaultFocus: "out",
  },
  warehouse: {
    title: "Warehouse",
    subtitle:
      "Dimensionamiento de almacen usando picking esperado derivado solo de entregas y validado contra backtest.",
    defaultFocus: "picking",
  },
};

export function DashboardPage({ pageMode }: { pageMode: PageMode }) {
  const [data, setData] = useState<DashboardData | null>(null);
  const [frequency, setFrequency] = useState<Frequency>(pageMode === "warehouse" ? "daily" : "weekly");
  const [quantile, setQuantile] = useState<Quantile>("p50");
  const [focus, setFocus] = useState<Focus>(pageConfig[pageMode].defaultFocus);
  const [rangePreset, setRangePreset] = useState<RangePreset>("180d");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    loadDashboardData().then((next) => {
      if (!mounted) {
        return;
      }
      setData(next);
      setLoading(false);
    });
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    setFocus(pageConfig[pageMode].defaultFocus);
    setFrequency(pageMode === "warehouse" ? "daily" : "weekly");
  }, [pageMode]);

  const filteredDaily = useMemo(
    () => applyRange(data?.daily ?? [], rangePreset, "daily"),
    [data?.daily, rangePreset],
  );
  const filteredWeekly = useMemo(
    () => applyRange(data?.weekly ?? [], rangePreset, "weekly"),
    [data?.weekly, rangePreset],
  );

  const chartPoints = useMemo(
    () => buildChartPoints(filteredDaily, filteredWeekly, frequency, quantile),
    [filteredDaily, filteredWeekly, frequency, quantile],
  );

  const kpis = useMemo(() => buildKpis(chartPoints), [chartPoints]);
  const backtest = useMemo(
    () => getBacktestHighlights(data?.backtest ?? [], frequency),
    [data?.backtest, frequency],
  );

  const latestPoint = chartPoints[chartPoints.length - 1];
  const exportName = `dashboard_${pageMode}_${frequency}_${quantile}.csv`;

  return (
    <Layout title={pageConfig[pageMode].title} subtitle={pageConfig[pageMode].subtitle}>
      <FilterBar
        frequency={frequency}
        onFrequencyChange={setFrequency}
        quantile={quantile}
        onQuantileChange={setQuantile}
        focus={focus}
        onFocusChange={setFocus}
        rangePreset={rangePreset}
        onRangePresetChange={setRangePreset}
        onExport={() => exportFilteredCsv(chartPoints, focus, exportName)}
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-9">
        {kpis.map((kpi) => (
          <KpiCard key={kpi.id} model={kpi} />
        ))}
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.55fr)_380px]">
        <ChartCard
          title={
            focus === "all"
              ? "Pulso general"
              : focus === "out"
                ? "Entregas OUT"
                : focus === "in"
                  ? "Recogidas IN"
                  : "Picking esperado"
          }
          subtitle="Serie principal filtrada para analisis operativo"
          data={chartPoints}
          focus={focus}
          frequency={frequency}
          quantile={quantile}
        />

        <div className="space-y-6">
          <DataStatus
            sources={
              data?.sources ?? {
                daily: "missing",
                weekly: "missing",
                backtest: "missing",
              }
            }
          />
          <SummaryCard
            title="Lectura rapida"
            lines={[
              { label: "Ultimo punto visible", value: latestPoint?.label ?? "sin datos" },
              { label: "WAPE servicio", value: formatPercent(backtest.serviceWape || 0) },
              { label: "WAPE picos servicio", value: formatPercent(backtest.servicePeakWape || 0) },
              { label: "WAPE picking esperado", value: formatPercent(backtest.pickingWape || 0) },
              { label: "Cobertura P80 picking", value: formatPercent(backtest.pickingCoverage || 0) },
            ]}
          />
          <SummaryCard
            title="Contexto de uso"
            lines={[
              { label: "Modo por defecto", value: "Static data" },
              { label: "Origen esperado", value: "/public/data + outputs" },
              { label: "Frecuencia actual", value: frequency },
              { label: "Escenario", value: quantile.toUpperCase() },
              { label: "Carga total visible", value: formatCompact(kpis.reduce((acc, item) => acc + item.value, 0)) },
              { label: "Estado carga", value: loading ? "cargando" : "listo" },
            ]}
          />
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-3">
        <ChartCard
          title="OUT"
          subtitle="Eventos, m3, pales y cajas"
          data={chartPoints}
          focus="out"
          frequency={frequency}
          quantile={quantile}
          compact
        />
        <ChartCard
          title="IN"
          subtitle="Recogidas para transporte y facturacion"
          data={chartPoints}
          focus="in"
          frequency={frequency}
          quantile={quantile}
          compact
        />
        <ChartCard
          title="Picking"
          subtitle="Solo entregas, desplazado por fecha de preparacion"
          data={chartPoints}
          focus="picking"
          frequency={frequency}
          quantile={quantile}
          compact
        />
      </div>
    </Layout>
  );
}
