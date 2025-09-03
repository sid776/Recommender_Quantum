// Minimal fetch helpers that hit your existing Ninja/Django endpoints
const json = async (res) => {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
};

export const getValuationBookCounts = () =>
  fetch('/api/valuation/reports/book_counts').then(json);

export const getRiskShocksCounts = () =>
  fetch('/api/riskshocks/counts').then(json);

export const getSensitivitiesBookCounts = () =>
  fetch('/api/sensitivities/book_counts').then(json);

export const getSensitivityPnl = () =>
  fetch('/api/sensitivity/pnl').then(json);

export const getValuationRunCounts = () =>
  fetch('/api/valuation/run_counts').then(json);
######################################################################################
// ---------------- Existing APIs ----------------
export const getRiskFactors = async (cobDate) => { ... };
export const getVarModels = async () => { ... };
export const triggerWorkflow = async (data) => { ... };

// ---------------- New APIs (valuation reports) ----------------

// GET /api/valuation/reports/book_counts
export const getValuationBookCounts = async () => {
  const res = await fetch(`/api/valuation/reports/book_counts`);
  if (!res.ok) throw new Error("Failed to fetch book counts");
  return res.json();
};

// GET /api/valuation/reports/riskshocks_counts
export const getRiskShocksCounts = async () => {
  const res = await fetch(`/api/valuation/reports/riskshocks_counts`);
  if (!res.ok) throw new Error("Failed to fetch risk shocks counts");
  return res.json();
};

// GET /api/valuation/reports/sensitivities/book_counts
export const getSensitivitiesBookCounts = async () => {
  const res = await fetch(`/api/valuation/reports/sensitivities/book_counts`);
  if (!res.ok) throw new Error("Failed to fetch sensitivities book counts");
  return res.json();
};

// GET /api/valuation/reports/sensitivity/pnl
export const getSensitivityPnl = async () => {
  const res = await fetch(`/api/valuation/reports/sensitivity/pnl`);
  if (!res.ok) throw new Error("Failed to fetch sensitivity PnL");
  return res.json();
};

// GET /api/valuation/reports/run_counts
export const getValuationRunCount = async () => {
  const res = await fetch(`/api/valuation/reports/run_counts`);
  if (!res.ok) throw new Error("Failed to fetch run count");
  return res.json();
};
#####################################################################################
import {
  getValuationBookCounts,
  getRiskShocksCounts,
  getSensitivitiesBookCounts,
  getSensitivityPnl,
  getValuationRunCount,
} from '../../api/api';
######################################################################################
const API = 'http://127.0.0.1:8000';  // single source of truth

const okJson = async (res) => {
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText} ${text && `- ${text.slice(0,120)}`}`);
  }
  return res.json();
};

// GET /api/valuation/reports/book_counts  -> [{ book, count }]
export const getValuationBookCounts = () =>
  fetch(`${API}/api/valuation/reports/book_counts`).then(okJson);

// GET /api/valuation/reports/riskshocks_counts  -> [{ risk_factor, curve, count }]
export const getRiskShocksCounts = () =>
  fetch(`${API}/api/valuation/reports/riskshocks_counts`).then(okJson);

// GET /api/valuation/reports/sensitivities/book_counts -> [{ book, count }]
export const getSensitivitiesBookCounts = () =>
  fetch(`${API}/api/valuation/reports/sensitivities/book_counts`).then(okJson);

// GET /api/valuation/reports/sensitivity/pnl -> [{ book, pnl, reval_pnl }]
export const getSensitivityPnl = () =>
  fetch(`${API}/api/valuation/reports/sensitivity/pnl`).then(okJson);

// GET /api/valuation/reports/run_counts -> { run_count: N }
export const getValuationRunCounts = () =>
  fetch(`${API}/api/valuation/reports/run_counts`).then(okJson);
