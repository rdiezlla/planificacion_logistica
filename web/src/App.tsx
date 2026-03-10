import { Navigate, Route, Routes } from "react-router-dom";
import { DashboardPage } from "./pages/DashboardPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage pageMode="overview" />} />
      <Route path="/transport" element={<DashboardPage pageMode="transport" />} />
      <Route path="/warehouse" element={<DashboardPage pageMode="warehouse" />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
