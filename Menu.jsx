import React from "react";
import { NavLink, useLocation } from "react-router-dom";

export default function Menu() {
  const { pathname } = useLocation();

  const linkCls = (isActive) =>
    `block rounded px-3 py-2 text-sm ${
      isActive ? "bg-emerald-700 text-white" : "hover:bg-gray-100"
    }`;

  const isSupportOpen =
    pathname.startsWith("/support");

  return (
    <nav className="px-3 py-2">
      {/* VaR (top-level) */}
      <div className="mb-2">
        <NavLink to="/var-calculator" className={({ isActive }) => linkCls(isActive)}>
          VaR
        </NavLink>
      </div>

      {/* Stress (top-level) */}
      <div className="mb-2">
        <NavLink to="/stress" className={({ isActive }) => linkCls(isActive)}>
          Stress
        </NavLink>
      </div>

      {/* Support (top-level label) */}
      <div className="mt-4">
        <div className="text-sm font-semibold text-gray-600 mb-1">Support</div>

        {/* Nested under Support */}
        <div className={`ml-2 space-y-1 ${isSupportOpen ? "" : ""}`}>
          <NavLink
            to="/support/cosmos-reports"
            className={({ isActive }) => linkCls(isActive)}
          >
            CosmosReports
          </NavLink>
        </div>
      </div>
    </nav>
  );
}
