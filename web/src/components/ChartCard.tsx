import {
  Area,
  AreaChart,
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChartPoint, Focus, Frequency, Quantile } from "../types";
import { formatNumber } from "../lib/format";

type Props = {
  title: string;
  subtitle: string;
  data: ChartPoint[];
  focus: Focus;
  frequency: Frequency;
  quantile: Quantile;
  compact?: boolean;
};

function tooltipFormatter(value: number) {
  return formatNumber(value, value % 1 === 0 ? 0 : 1);
}

export function ChartCard({ title, subtitle, data, focus, frequency, quantile, compact }: Props) {
  const height = compact ? 220 : 340;

  const commonChart = (
    <>
      <CartesianGrid stroke="#ece6dd" strokeDasharray="2 6" vertical={false} />
      <XAxis
        dataKey="label"
        tickLine={false}
        axisLine={false}
        minTickGap={18}
        tick={{ fill: "#6b7280", fontSize: 12 }}
      />
      <YAxis
        tickLine={false}
        axisLine={false}
        tick={{ fill: "#6b7280", fontSize: 12 }}
        width={44}
      />
      <Tooltip
        contentStyle={{
          borderRadius: 18,
          border: "1px solid #e7e0d7",
          boxShadow: "0 12px 30px rgba(17,24,39,0.08)",
          background: "rgba(255,255,255,0.97)",
        }}
        formatter={(value: number) => tooltipFormatter(Number(value))}
      />
      <Legend />
    </>
  );

  return (
    <div className="rounded-[30px] border border-white/70 bg-white/90 p-5 shadow-card">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <p className="text-lg font-semibold text-ink">{title}</p>
          <p className="mt-1 text-sm text-smoke">
            {subtitle} | {frequency === "daily" ? "serie diaria" : "serie semanal"} | {quantile.toUpperCase()}
          </p>
        </div>
        <div className="rounded-full bg-fog px-3 py-1 text-xs uppercase tracking-[0.2em] text-smoke">
          {focus === "all" ? "mixto" : focus}
        </div>
      </div>

      <div style={{ width: "100%", height }}>
        <ResponsiveContainer>
          {focus === "picking" ? (
            <ComposedChart data={data}>
              {commonChart}
              <Bar dataKey="picking" name="Picking esperado" fill="#6bc8a3" radius={[8, 8, 0, 0]} />
              {frequency === "weekly" ? (
                <Line
                  type="monotone"
                  dataKey="pickingReal"
                  name="Picking real"
                  stroke="#111827"
                  strokeWidth={2}
                  dot={false}
                />
              ) : null}
            </ComposedChart>
          ) : focus === "in" ? (
            <LineChart data={data}>
              {commonChart}
              <Line type="monotone" dataKey="inEvents" name="Eventos IN" stroke="#5f90ff" strokeWidth={3} dot={false} />
              <Line type="monotone" dataKey="inM3" name="m3 IN" stroke="#111827" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="inPales" name="Pales IN" stroke="#7ca9ff" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="inCajas" name="Cajas IN" stroke="#9dbbff" strokeWidth={2} dot={false} />
            </LineChart>
          ) : focus === "out" ? (
            <LineChart data={data}>
              {commonChart}
              <Line type="monotone" dataKey="outEvents" name="Eventos OUT" stroke="#eb6a9b" strokeWidth={3} dot={false} />
              <Line type="monotone" dataKey="outM3" name="m3 OUT" stroke="#111827" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="outPales" name="Pales OUT" stroke="#f08db2" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="outCajas" name="Cajas OUT" stroke="#f5b4cc" strokeWidth={2} dot={false} />
            </LineChart>
          ) : (
            <AreaChart data={data}>
              {commonChart}
              <Area
                type="monotone"
                dataKey="outEvents"
                name="OUT eventos"
                stackId="1"
                stroke="#eb6a9b"
                fill="#eb6a9b"
                fillOpacity={0.24}
              />
              <Area
                type="monotone"
                dataKey="inEvents"
                name="IN eventos"
                stackId="2"
                stroke="#5f90ff"
                fill="#5f90ff"
                fillOpacity={0.2}
              />
              <Line type="monotone" dataKey="picking" name="Picking esperado" stroke="#2d8f68" strokeWidth={3} dot={false} />
            </AreaChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
