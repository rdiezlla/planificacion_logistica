type Props = {
  title: string;
  lines: Array<{ label: string; value: string }>;
};

export function SummaryCard({ title, lines }: Props) {
  return (
    <div className="rounded-[28px] border border-white/70 bg-white/90 p-5 shadow-card">
      <p className="text-lg font-semibold text-ink">{title}</p>
      <div className="mt-4 space-y-3">
        {lines.map((line) => (
          <div
            key={line.label}
            className="flex items-center justify-between gap-4 border-b border-line/70 pb-3 last:border-b-0 last:pb-0"
          >
            <span className="text-sm text-smoke">{line.label}</span>
            <span className="text-sm font-semibold text-ink">{line.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
