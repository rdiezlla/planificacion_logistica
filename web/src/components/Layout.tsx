import type { ReactNode } from "react";
import { Sidebar } from "./Sidebar";

export function Layout({
  title,
  subtitle,
  showHeader = true,
  compact = false,
  children,
}: {
  title: string;
  subtitle: string;
  showHeader?: boolean;
  compact?: boolean;
  children: ReactNode;
}) {
  return (
    <div className="min-h-screen p-3 md:p-4">
      <div className="mx-auto grid max-w-[1680px] gap-4 xl:grid-cols-[96px_minmax(0,1fr)]">
        <Sidebar />
        <main className={["rounded-[28px] border border-white/70 bg-white/55 shadow-shell backdrop-blur", compact ? "p-3 md:p-4" : "space-y-6 p-4 md:p-6"].join(" ")}>
          {showHeader ? (
            <header className="mb-6 flex flex-col gap-4 rounded-[28px] border border-white/60 bg-white/75 px-5 py-5 md:flex-row md:items-end md:justify-between">
              <div>
                <div className="text-xs uppercase tracking-[0.24em] text-smoke">Logistics planning</div>
                <h1 className="mt-2 font-serif text-4xl text-ink">{title}</h1>
                <p className="mt-2 max-w-3xl text-sm text-smoke">{subtitle}</p>
              </div>
              <div className="rounded-[22px] bg-ink px-4 py-3 text-sm text-white">
                Dashboard web principal
              </div>
            </header>
          ) : null}
          {children}
        </main>
      </div>
    </div>
  );
}
