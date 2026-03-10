import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const outputsDir = path.resolve(__dirname, "..", "..", "outputs");
const port = Number(process.env.PORT || 8787);

const allowedFiles = new Set([
  "forecast_weekly_business.csv",
  "forecast_daily_business.csv",
  "backtest_metrics.csv",
]);

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/data/:filename", (req, res) => {
  const filename = req.params.filename;
  if (!allowedFiles.has(filename)) {
    res.status(404).json({ error: "file_not_allowed" });
    return;
  }

  const filePath = path.join(outputsDir, filename);
  if (!fs.existsSync(filePath)) {
    res.status(404).json({ error: "file_not_found" });
    return;
  }

  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  fs.createReadStream(filePath).pipe(res);
});

app.listen(port, () => {
  console.log(`Dashboard optional server listening on http://localhost:${port}`);
});
