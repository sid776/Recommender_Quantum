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
