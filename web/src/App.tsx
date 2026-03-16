import { Navigate, Route, Routes } from "react-router-dom";
import { DashboardPage } from "./pages/DashboardPage";
import { SupervisorPage } from "./pages/SupervisorPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<SupervisorPage />} />
      <Route path="/legacy/overview" element={<DashboardPage pageMode="overview" />} />
      <Route path="/legacy/transport" element={<DashboardPage pageMode="transport" />} />
      <Route path="/legacy/warehouse" element={<DashboardPage pageMode="warehouse" />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
