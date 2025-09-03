// frontend/src/api/api.js
// One base URL (no proxy). Change if your Django runs elsewhere.
const API = 'http://127.0.0.1:8000';

// Helper: fetch→JSON with readable errors
const okJson = async (res) => {
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? ` - ${text.slice(0, 200)}` : ''}`);
  }
  return res.json();
};

// Helper: build URL with optional query params
const url = (path, params = {}) => {
  const u = new URL(`${API}${path}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, v);
  });
  return u.toString();
};

/* =========================================================================
 * EXISTING APIs (under /api/frontend/...)
 * ========================================================================= */

// GET /api/frontend/riskfactor/all[?cob_date=YYYY-MM-DD]
export const getRiskFactors = (cobDate) =>
  fetch(url('/api/frontend/riskfactor/all', cobDate ? { cob_date: cobDate } : {})).then(okJson);

// GET /api/frontend/varmodel/all
export const getVarModels = () =>
  fetch(url('/api/frontend/varmodel/all')).then(okJson);

// POST /api/frontend/workflow/submit
export const triggerWorkflow = (data) =>
  fetch(url('/api/frontend/workflow/submit'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }).then(okJson);

// GET /api/frontend/dropdown?names=var_type
export const getVarTypes = () =>
  fetch(url('/api/frontend/dropdown', { names: 'var_type' })).then(okJson);

// GET /api/frontend/varmodel/byname?name=XYZ
export const getVarModelByName = (name) =>
  fetch(url('/api/frontend/varmodel/byname', { name })).then(okJson);

// GET /api/frontend/runrequest/all?request_date=YYYY-MM-DD
export const getRunRequestDetails = (jobTriggeredDate) =>
  fetch(url('/api/frontend/runrequest/all', { request_date: jobTriggeredDate })).then(okJson);

// POST /api/frontend/varmodel/save
export const varModelSave = (data) =>
  fetch(url('/api/frontend/varmodel/save'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }).then(okJson);

// GET /api/frontend/details?model=products
export const getProducts = () =>
  fetch(url('/api/frontend/details', { model: 'products' })).then(okJson);

// GET /api/frontend/details?model=entities
export const getEntities = () =>
  fetch(url('/api/frontend/details', { model: 'entities' })).then(okJson);

// Download blob: GET /api/frontend/workflow/export?request_id=123
export const exportWorkflowRequest = async (request_id) => {
  const res = await fetch(url('/api/frontend/workflow/export', { request_id }));
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${text ? ` - ${text.slice(0, 200)}` : ''}`);
  }
  const blob = await res.blob();
  const a = document.createElement('a');
  a.href = window.URL.createObjectURL(blob);
  a.download = `${request_id}.json`;
  a.click();
  window.URL.revokeObjectURL(a.href);
};

// GET /api/frontend/hierarchy?name=books
export const getBookHierarchy = () =>
  fetch(url('/api/frontend/hierarchy', { name: 'books' })).then(okJson);

// GET /api/frontend/hierarchy?name=products
export const getProductsHierarchy = () =>
  fetch(url('/api/frontend/hierarchy', { name: 'products' })).then(okJson);

// GET /api/frontend/runrequest/byuser
export const getRunRequestByUser = () =>
  fetch(url('/api/frontend/runrequest/byuser')).then(okJson);

// GET /api/frontend/var/results?cob_date=YYYY-MM-DD
export const getVarResults = (cob_date) =>
  fetch(url('/api/frontend/var/results', { cob_date })).then(okJson);

// GET /api/frontend/benchmarking-url
export const getBenchmarkingToolUrl = () =>
  fetch(url('/api/frontend/benchmarking-url')).then(okJson);

/* =========================================================================
 * NEW Valuation Report APIs (under /api/valuation/reports/...)
 * ========================================================================= */

// GET /api/valuation/reports/book_counts  → [{ book, count }]
export const getValuationBookCounts = () =>
  fetch(url('/api/valuation/reports/book_counts')).then(okJson);

// GET /api/valuation/reports/riskshocks_counts  → [{ risk_factor, curve, count }]
export const getRiskShocksCounts = () =>
  fetch(url('/api/valuation/reports/riskshocks_counts')).then(okJson);

// GET /api/valuation/reports/sensitivities/book_counts  → [{ book, count }]
export const getSensitivitiesBookCounts = () =>
  fetch(url('/api/valuation/reports/sensitivities/book_counts')).then(okJson);

// GET /api/valuation/reports/sensitivity/pnl  → [{ book, pnl, reval_pnl }]
export const getSensitivityPnl = () =>
  fetch(url('/api/valuation/reports/sensitivity/pnl')).then(okJson);

// GET /api/valuation/reports/run_counts  → { run_count: N }
export const getValuationRunCounts = () =>
  fetch(url('/api/valuation/reports/run_counts')).then(okJson);
