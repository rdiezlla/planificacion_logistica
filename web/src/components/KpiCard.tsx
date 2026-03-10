import type { KpiCardModel } from "../types";
import { formatNumber } from "../lib/format";

const toneClasses: Record<KpiCardModel["tone"], string> = {
  out: "from-coral/20 to-coral/5 text-coral",
  in: "from-sky/20 to-sky/5 text-sky",
  picking: "from-mint/25 to-mint/5 text-emerald-700",
};

export function KpiCard({ model }: { model: KpiCardModel }) {
  return (
    <div className="rounded-[24px] border border-white/70 bg-white/85 p-4 shadow-card backdrop-blur">
      <div
        className={`mb-4 inline-flex rounded-full bg-gradient-to-r px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${toneClasses[model.tone]}`}
      >
        {model.label}
      </div>
      <div className="text-3xl font-semibold text-ink">
        {formatNumber(model.value, model.decimals ?? 0)}
        {model.suffix ? <span className="ml-1 text-base text-smoke">{model.suffix}</span> : null}
      </div>
    </div>
  );
}
