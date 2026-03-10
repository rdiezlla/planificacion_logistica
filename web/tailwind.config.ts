import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#111827",
        panel: "#fbfaf7",
        line: "#ded8d0",
        fog: "#f3eee6",
        mint: "#6bc8a3",
        coral: "#eb6a9b",
        sky: "#5f90ff",
        amber: "#d89f40",
        smoke: "#6b7280",
      },
      boxShadow: {
        shell: "0 28px 80px rgba(17, 24, 39, 0.10)",
        card: "0 14px 36px rgba(17, 24, 39, 0.08)",
      },
      fontFamily: {
        sans: ["Aptos", "\"Segoe UI Variable\"", "\"Segoe UI\"", "system-ui", "sans-serif"],
        serif: ["\"Iowan Old Style\"", "Georgia", "serif"],
      },
      backgroundImage: {
        grain:
          "radial-gradient(circle at 1px 1px, rgba(17,24,39,0.05) 1px, transparent 0)",
      },
    },
  },
  plugins: [],
} satisfies Config;
