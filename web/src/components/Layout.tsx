import type { ReactNode } from "react";
import { Sidebar } from "./Sidebar";

export function Layout({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: ReactNode;
}) {
  return (
    <div className="min-h-screen p-4 md:p-6">
      <div className="mx-auto grid max-w-[1680px] gap-6 xl:grid-cols-[280px_minmax(0,1fr)]">
        <Sidebar />
        <main className="space-y-6 rounded-[34px] border border-white/70 bg-white/55 p-4 shadow-shell backdrop-blur md:p-6">
          <header className="flex flex-col gap-4 rounded-[30px] border border-white/60 bg-white/75 px-5 py-5 md:flex-row md:items-end md:justify-between">
            <div>
              <div className="text-xs uppercase tracking-[0.24em] text-smoke">Logistics planning</div>
              <h1 className="mt-2 font-serif text-4xl text-ink">{title}</h1>
              <p className="mt-2 max-w-3xl text-sm text-smoke">{subtitle}</p>
            </div>
            <div className="rounded-[24px] bg-ink px-4 py-3 text-sm text-white">
              Static-first dashboard listo para CSV de negocio
            </div>
          </header>
          {children}
        </main>
      </div>
    </div>
  );
}
