import { NavLink } from "react-router-dom";

const navItems = [
  { to: "/", label: "Overview", description: "Vista ejecutiva" },
  { to: "/transport", label: "Transport", description: "OUT e IN" },
  { to: "/warehouse", label: "Warehouse", description: "Picking esperado" },
];

export function Sidebar() {
  return (
    <aside className="flex min-h-[280px] flex-col rounded-[28px] border border-white/70 bg-white/80 p-5 shadow-card backdrop-blur">
      <div className="mb-8">
        <p className="font-serif text-2xl text-ink">Planning Hub</p>
        <p className="mt-2 text-sm text-smoke">
          Transporte y almacen con lectura directa de `outputs`.
        </p>
      </div>

      <div className="space-y-2">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              [
                "block rounded-2xl border px-4 py-3 transition",
                isActive
                  ? "border-ink bg-ink text-white shadow-card"
                  : "border-transparent bg-fog/80 text-ink hover:border-line hover:bg-white",
              ].join(" ")
            }
          >
            <div className="text-sm font-semibold">{item.label}</div>
            <div className="mt-1 text-xs opacity-70">{item.description}</div>
          </NavLink>
        ))}
      </div>

      <div className="mt-auto rounded-3xl bg-gradient-to-br from-ink to-slate-700 p-4 text-white">
        <p className="text-xs uppercase tracking-[0.2em] text-white/60">Static mode</p>
        <p className="mt-2 text-sm leading-6 text-white/85">
          Por defecto carga CSV desde `public/data`. Si faltan, entra en modo mock para no romper
          el dashboard.
        </p>
      </div>
    </aside>
  );
}
