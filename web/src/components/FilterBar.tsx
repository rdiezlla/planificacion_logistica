import type { Focus, Frequency, Quantile, RangePreset } from "../types";

type Props = {
  frequency: Frequency;
  onFrequencyChange: (value: Frequency) => void;
  quantile: Quantile;
  onQuantileChange: (value: Quantile) => void;
  focus: Focus;
  onFocusChange: (value: Focus) => void;
  rangePreset: RangePreset;
  onRangePresetChange: (value: RangePreset) => void;
  onExport: () => void;
};

function FilterGroup<T extends string>({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: T;
  onChange: (value: T) => void;
  options: { value: T; label: string }[];
}) {
  return (
    <div>
      <div className="mb-2 text-[11px] uppercase tracking-[0.22em] text-smoke">{label}</div>
      <div className="flex flex-wrap gap-2">
        {options.map((option) => (
          <button
            key={option.value}
            type="button"
            onClick={() => onChange(option.value)}
            className={[
              "rounded-full border px-3 py-2 text-sm transition",
              option.value === value
                ? "border-ink bg-ink text-white"
                : "border-line bg-white text-ink hover:border-ink/40",
            ].join(" ")}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export function FilterBar(props: Props) {
  return (
    <div className="rounded-[28px] border border-white/70 bg-white/85 p-5 shadow-card backdrop-blur">
      <div className="grid gap-5 xl:grid-cols-[1fr_1fr_1fr_auto]">
        <FilterGroup
          label="Frecuencia"
          value={props.frequency}
          onChange={props.onFrequencyChange}
          options={[
            { value: "weekly", label: "Weekly" },
            { value: "daily", label: "Daily" },
          ]}
        />
        <FilterGroup
          label="Escenario"
          value={props.quantile}
          onChange={props.onQuantileChange}
          options={[
            { value: "p50", label: "P50 base" },
            { value: "p80", label: "P80 alto" },
          ]}
        />
        <div className="grid gap-4 md:grid-cols-2">
          <FilterGroup
            label="Vista"
            value={props.focus}
            onChange={props.onFocusChange}
            options={[
              { value: "all", label: "All" },
              { value: "out", label: "OUT" },
              { value: "in", label: "IN" },
              { value: "picking", label: "Picking" },
            ]}
          />
          <FilterGroup
            label="Rango"
            value={props.rangePreset}
            onChange={props.onRangePresetChange}
            options={[
              { value: "30d", label: "30d" },
              { value: "90d", label: "90d" },
              { value: "180d", label: "180d" },
              { value: "365d", label: "365d" },
              { value: "all", label: "All" },
            ]}
          />
        </div>
        <div className="flex items-end justify-end">
          <button
            type="button"
            onClick={props.onExport}
            className="rounded-full border border-ink bg-ink px-4 py-3 text-sm font-semibold text-white transition hover:translate-y-[-1px]"
          >
            Exportar CSV filtrado
          </button>
        </div>
      </div>
    </div>
  );
}
