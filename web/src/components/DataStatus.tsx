import type { DashboardData } from "../types";

function sourceTone(source: DashboardData["sources"][keyof DashboardData["sources"]]) {
  if (source === "csv") {
    return "bg-emerald-100 text-emerald-800";
  }
  if (source === "mock") {
    return "bg-amber-100 text-amber-800";
  }
  return "bg-rose-100 text-rose-800";
}

export function DataStatus({ sources }: { sources: DashboardData["sources"] }) {
  const items = [
    { label: "Daily business", value: sources.daily },
    { label: "Weekly business", value: sources.weekly },
    { label: "Backtest", value: sources.backtest },
  ];

  return (
    <div className="rounded-[28px] border border-white/70 bg-white/85 p-5 shadow-card">
      <p className="text-sm font-semibold text-ink">Estado de fuentes</p>
      <div className="mt-4 flex flex-wrap gap-3">
        {items.map((item) => (
          <div key={item.label} className="rounded-2xl border border-line bg-fog/60 px-4 py-3">
            <div className="text-xs uppercase tracking-[0.18em] text-smoke">{item.label}</div>
            <div
              className={`mt-2 inline-flex rounded-full px-3 py-1 text-xs font-semibold ${sourceTone(item.value)}`}
            >
              {item.value}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
