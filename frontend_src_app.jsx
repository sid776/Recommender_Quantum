import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Menu from "./components/Menu.jsx";

// Pages already in your app
import Calculator from "./components/pages/Calculator/index.jsx";
import Stress from "./components/pages/Stress/index.jsx";
import CosmosReports from "./components/pages/CosmosReports/index.jsx";
import Support from "./components/pages/Support/index.jsx";

function Layout({ children }) {
  return (
    <div className="w-full h-full flex">
      <aside className="w-64 bg-white border-r">
        <Menu />
      </aside>
      <main className="flex-1 overflow-auto">{children}</main>
    </div>
  );
}

export default function App() {
  return (
    <Layout>
      <Routes>
        {/* Top-level VaR and Stress */}
        <Route path="/var-calculator" element={<Calculator />} />
        <Route path="/stress" element={<Stress />} />

        {/* Support as top-level section; CosmosReports under Support */}
        <Route path="/support" element={<Support />}>
          <Route index element={<Navigate to="/support/cosmos-reports" replace />} />
          <Route path="cosmos-reports" element={<CosmosReports />} />
        </Route>

        {/* Optional legacy redirects */}
        <Route path="/cosmos-reports" element={<Navigate to="/support/cosmos-reports" replace />} />

        {/* Default landing */}
        <Route path="/" element={<Navigate to="/var-calculator" replace />} />
        <Route path="*" element={<Navigate to="/var-calculator" replace />} />
      </Routes>
    </Layout>
  );
}
