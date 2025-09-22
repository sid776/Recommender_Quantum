// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const FIXED_YEARS = ["2021", "2022", "2023", "2024", "2025"];

const lc = (s) => String(s || "").toLowerCase();
const prettify = (k) => String(k).replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v == null || v === "";

/* Always show these, populated from first matching key found in the data */
const PREFERRED_FIELDS = [
  { header: "Rule Type",       keys: ["rule_type", "ruletype"],          target: "rule_type" },
  { header: "Risk Factor Id",  keys: ["risk_factor_id", "risk factor id", "rf_id", "riskfactorid"], target: "risk_factor_id" },
  { header: "Stale Date",      keys: ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"], target: "stale_date" },
  { header: "Rule Logic",      keys: ["rule_logic", "logic", "rule"],    target: "rule_logic" },
  { header: "Z Score",         keys: ["z_score", "zscore", "z score"],   target: "z_score" },
  { header: "Std Value",       keys: ["std_value", "std value", "std", "stddev", "std_dev"], target: "std_value" },
  { header: "Mean Value",      keys: ["mean_value", "mean value", "mean"], target: "mean_value" },
  { header: "Is Outlier",      keys: ["is_outlier", "is outlier", "outlier"], target: "is_outlier" },
  { header: "Unique Tag",      keys: ["unique_tag", "unique tag", "uniquetag"], target: "unique_tag" },
];

/** ---- fuzzy matching helpers (handles prefixes like "1.2 std_value") ---- **/
const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9_]/g, "");
function keyMatches(actualKey, candidate) {
  const a = norm(actualKey);
  const c = norm(candidate);
  return a === c || a.endsWith(c); // allow suffix match (e.g., "12std_value" endsWith "std_value")
}
function pickKey(row, candidates) {
  if (!row) return null;
  const keys = Object.keys(row);
  let best = null;
  let bestLen = -1;
  for (const k of keys) {
    for (const cand of candidates || []) {
      if (keyMatches(k, cand)) {
        const len = norm(cand).length;
        if (len > bestLen) { best = k; bestLen = len; }
      }
    }
  }
  return best;
}
function findKeyInRows(rows, candidates) {
  if (!rows?.length) return null;
  const tally = new Map();
  for (const r of rows) {
    const k = pickKey(r, candidates);
    if (k) tally.set(k, (tally.get(k) || 0) + 1);
  }
  let best = null, max = 0;
  for (const [k, cnt] of tally.entries()) if (cnt > max) { best = k; max = cnt; }
  return best;
}
/** ----------------------------------------------------------------------- **/

/* latest-date helpers for defaulting COB */
function parseYMD(v) {
  if (!v && v !== 0) return null;
  const s = String(v).trim().replace(/['"]/g, "");
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  return null;
}
function maxDateStr(dates) {
  let max = null;
  for (const d of dates) {
    const normed = parseYMD(d);
    if (!normed) continue;
    if (!max || normed > max) max = normed;
  }
  return max;
}

/**
 * Compute a map of { targetField -> actualSourceKey } once per dataset,
 * then copy values into **safe synthetic fields** so ag-Grid can bind with `field`.
 */
function buildSourceKeyMap(rows) {
  const map = {};
  for (const pf of PREFERRED_FIELDS) {
    map[pf.target] = findKeyInRows(rows, pf.keys) || null;
  }
  // also remember a book key (for grouping)
  map.__book = findKeyInRows(rows, ["book", "book_nm", "book name"]) || null;
  return map;
}

/* normalize rows (numbers, booleans, year columns) + copy to safe fields */
function normalizeRows(rows, sourceMap) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    // Map Y2021..Y2025 → 2021..2025, keep 0 if missing
    FIXED_YEARS.forEach((y) => {
      const yPlain = Number(o[y]);
      const yPref = Number(o[`Y${y}`]);
      if (Number.isFinite(yPlain)) o[y] = yPlain;
      else if (Number.isFinite(yPref)) o[y] = yPref;
      else o[y] = 0;
      if (`Y${y}` in o) delete o[`Y${y}`];
    });

    // derive hidden book grouping
    o.__book = sourceMap.__book ? o[sourceMap.__book] : "";

    // copy preferred fields into **safe** canonical keys
    for (const pf of PREFERRED_FIELDS) {
      const src = sourceMap[pf.target];
      if (src && src in o) {
        o[pf.target] = o[src];
      } else {
        // per spec: show blank rather than undefined
        if (!(pf.target in o)) o[pf.target] = null;
      }
    }

    // if a date-ish preferred field is blank, fall back to report_date
    const reportDate = o.report_date || "";
    if (isNilOrEmpty(o.stale_date)) o.stale_date = reportDate || "—";

    // numeric coercion for our **safe** numeric keys
    ["z_score", "std_value", "mean_value"].forEach((k) => {
      if (k in o) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : (o[k] ?? 0);
      }
    });

    // boolean-ish for our **safe** is_outlier
    if ("is_outlier" in o) {
      o.is_outlier = o.is_outlier ? 1 : 0;
    }

    return o;
  });
}

/* simple aggregators so grouped rows don't look empty */
const aggFirst = (params) => {
  for (const v of params.values || []) if (v !== undefined && v !== null && v !== "") return v;
  return null;
};
const aggAvg = (params) => {
  const vals = (params.values || []).map(Number).filter((n) => Number.isFinite(n));
  if (!vals.length) return null;
  return vals.reduce((a, b) => a + b, 0) / vals.length;
};

/* build columns (bind to the **safe** fields) */
function buildColumnDefs() {
  const groupCols = [
    { headerName: "Report Type", field: "report_type", rowGroup: true, rowGroupIndex: 0, hide: true },
    { headerName: "Book",        field: "__book",      rowGroup: true, rowGroupIndex: 1, hide: true },
  ];

  const reportDateCol = {
    headerName: "Report Date",
    field: "report_date",
    sortable: true,
    resizable: true,
    minWidth: 150,
    filter: "agTextColumnFilter",
    aggFunc: aggFirst,
  };

  const yearCols = FIXED_YEARS.map((y) => ({
    headerName: y,
    field: y,
    type: "numericColumn",
    filter: "agNumberColumnFilter",
    aggFunc: "sum",
    sortable: true,
    resizable: true,
    minWidth: 110,
    valueParser: (p) => Number(p.newValue ?? 0),
  }));

  const numericSafeSet = new Set(["z_score", "std_value", "mean_value"]);

  const preferredCols = PREFERRED_FIELDS.map(({ header, target }) => ({
    headerName: header,
    field: target,                           // bind to our **safe** key
    sortable: true,
    resizable: true,
    headerTooltip: header,
    minWidth: 160,
    filter: numericSafeSet.has(target) ? "agNumberColumnFilter" : "agTextColumnFilter",
    aggFunc: numericSafeSet.has(target) ? aggAvg : aggFirst,
  }));

  // Final order: group (hidden) → report date → years → preferred
  return [...groupCols, reportDateCol, ...yearCols, ...preferredCols];
}

export default function CosmosReports() {
  const methods = useForm({ defaultValues: { report_date: "" } });
  const { setValue, control } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);

  const gridRef = useRef(null);
  const fetchedOnce = useRef(false);

  const sourceMap = useMemo(() => buildSourceKeyMap(rows), [rows]);
  const normRows  = useMemo(() => normalizeRows(rows, sourceMap), [rows, sourceMap]);
  const columnDefs = useMemo(() => buildColumnDefs(), []);

  const reportDate = useWatch({ control, name: "report_date" });

  async function fetchData(dateStr) {
    setLoading(true);
    try {
      const url = dateStr
        ? `${API_ENDPOINT}?report_date=${encodeURIComponent(dateStr)}&limit=500`
        : `${API_ENDPOINT}?limit=500`;
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) { setRows([]); return; }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);
      if ((!dateStr || !dateStr.length) && data?.length) {
        const latest = maxDateStr(data.map((r) => r?.report_date).filter(Boolean));
        if (latest) setValue("report_date", latest, { shouldDirty: false });
      }
    } finally {
      setLoading(false);
      requestAnimationFrame(() => {
        const api = gridRef.current?.api;
        const columnApi = gridRef.current?.columnApi;
        if (!api || !columnApi) return;
        const ids = [];
        columnApi.getColumns()?.forEach((c) => ids.push(c.getColId()));
        columnApi.autoSizeColumns(ids, true);
        api.setGridOption("suppressAggFuncInHeader", true);
      });
    }
  }

  useEffect(() => {
    if (fetchedOnce.current) return;
    fetchedOnce.current = true;
    fetchData("");
  }, []);

  useEffect(() => {
    if (!fetchedOnce.current) return;
    if (!reportDate) return;
    fetchData(reportDate);
  }, [reportDate]);

  const onFirstDataRendered = () => {
    const api = gridRef.current?.api;
    const columnApi = gridRef.current?.columnApi;
    if (!api || !columnApi) return;
    const ids = [];
    columnApi.getColumns()?.forEach((c) => ids.push(c.getColId()));
    columnApi.autoSizeColumns(ids, true);
    api.sizeColumnsToFit({ defaultMinWidth: 110 });
    api.setGridOption("suppressAggFuncInHeader", true);
  };

  const onGridReady = (params) => {
    params.api.setSideBarVisible(true);
    params.api.closeToolPanel();
  };

  return (
    <Box className="overflow-hidden" height="calc(100vh - 70px)">
      <style>{`
        .custom-grid .ag-side-buttons {
          display: flex;
          flex-direction: column;
          justify-content: center;
        }
      `}</style>

      <FormProvider {...methods}>
        <form className="flex flex-col gap-3 h-full">
          <div className="p-4 bg-white shadow-md rounded-lg flex flex-col h-full">
            <div className="flex items-center justify-between mb-3">
              <div className="text-lg font-bold">DQ Reports</div>
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <span className="text-base font-semibold text-gray-700">COB:</span>
                  <div className="w-[220px]">
                    <InputFieldset
                      id="report_date"
                      label=""
                      fieldName="report_date"
                      tooltipMsg="COB"
                      type="date"
                      required
                      registerOptions={{ required: "required" }}
                    />
                  </div>
                </div>
                <i
                  title="Global Filter"
                  className="ph ph-funnel cursor-pointer text-green-700"
                  style={{ fontSize: 28 }}
                  onClick={() => setShowFloatingFilters((v) => !v)}
                />
              </div>
            </div>

            <Box
              className="ag-theme-alpine rounded-lg flex-1 custom-grid"
              style={{
                width: "100%",
                border: "none",
                ["--ag-borders"]: "none",
                ["--ag-border-color"]: "transparent",
              }}
            >
              <AgGridReact
                ref={gridRef}
                rowData={normRows}
                columnDefs={columnDefs}
                defaultColDef={{
                  flex: 1,
                  minWidth: 120,
                  sortable: true,
                  filter: true,
                  resizable: true,
                  enableValue: true,
                  enableRowGroup: true,
                  enablePivot: true,
                  enableCharts: true,
                  floatingFilter: showFloatingFilters,
                }}
                autoGroupColumnDef={{
                  headerName: "Group",
                  minWidth: 260,
                  pinned: "left",
                  valueGetter: (p) =>
                    p?.node?.key ? `${p.node.key} (${p.node.allLeafChildren?.length || 0})` : "",
                }}
                headerHeight={42}
                floatingFiltersHeight={36}
                loading={loading}
                animateRows
                enableRangeSelection
                suppressAggFuncInHeader
                onFirstDataRendered={onFirstDataRendered}
                onGridReady={onGridReady}
                suppressHorizontalScroll={false}
                sideBar={{
                  position: "right",
                  hiddenByDefault: false,
                  toolPanels: [
                    { id: "columns", labelDefault: "Columns", iconKey: "columns", toolPanel: "agColumnsToolPanel" },
                    { id: "filters", labelDefault: "Filters", iconKey: "filter", toolPanel: "agFiltersToolPanel" },
                  ],
                  defaultToolPanel: null,
                }}
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
