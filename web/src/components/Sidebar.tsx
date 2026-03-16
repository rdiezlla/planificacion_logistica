import { NavLink } from "react-router-dom";

const navItems = [
  {
    to: "/",
    label: "Supervisor",
    short: "SV",
    icon: (
      <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.8">
        <path d="M4 18h16" />
        <path d="M6 15V9" />
        <path d="M12 15V6" />
        <path d="M18 15v-3" />
      </svg>
    ),
  },
  {
    to: "/legacy/transport",
    label: "Transport",
    short: "TR",
    icon: (
      <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.8">
        <rect x="3" y="7" width="14" height="9" rx="1.5" />
        <path d="M17 10h3l1 2v4h-4" />
        <circle cx="7" cy="17" r="1.5" />
        <circle cx="18" cy="17" r="1.5" />
      </svg>
    ),
  },
  {
    to: "/legacy/warehouse",
    label: "Warehouse",
    short: "WH",
    icon: (
      <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.8">
        <path d="M3 9 12 4l9 5v10H3z" />
        <path d="M9 20v-6h6v6" />
      </svg>
    ),
  },
];

export function Sidebar() {
  return (
    <aside className="flex min-h-[86vh] flex-col items-center rounded-[24px] border border-white/70 bg-white/82 p-3 shadow-card backdrop-blur">
      <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-2xl bg-ink text-white shadow-card">
        <span className="text-xs font-bold tracking-[0.2em]">PL</span>
      </div>

      <div className="flex w-full flex-col items-center gap-2">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              [
                "flex w-full flex-col items-center rounded-2xl border px-1 py-2 transition",
                isActive
                  ? "border-ink bg-ink text-white shadow-card"
                  : "border-transparent bg-fog/70 text-ink hover:border-line hover:bg-white",
              ].join(" ")
            }
            title={item.label}
          >
            <span className="mb-1">{item.icon}</span>
            <span className="text-[10px] font-semibold uppercase tracking-[0.16em]">{item.short}</span>
          </NavLink>
        ))}
      </div>

      <div className="mt-auto w-full rounded-2xl bg-gradient-to-br from-fog to-white px-2 py-2 text-center">
        <p className="text-[9px] uppercase tracking-[0.18em] text-smoke">Semana</p>
        <p className="mt-1 text-[10px] leading-4 text-ink">Forecast / 2024 / Real 2026</p>
      </div>
    </aside>
  );
}
