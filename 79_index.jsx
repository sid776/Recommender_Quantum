// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box, Button } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const FIXED_YEARS = ["2021", "2022", "2023", "2024", "2025"];

const lc = (s) => String(s || "").toLowerCase();
const prettify = (k) => String(k).replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v == null || v === "";
const normKey = (s) => lc(s).replace(/[^a-z0-9]/g, "");

/* Preferred (append after year columns) */
const PREFERRED_FIELDS = [
  { header: "Rule Type",       keys: ["rule_type", "ruletype"] },
  { header: "Risk Factor Id",  keys: ["risk_factor_id", "risk factor id", "rf_id", "riskfactorid"] },
  { header: "Stale Date",      keys: ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"] },
  { header: "As Of Date",      keys: ["as_of_date", "as_of_dt", "asofdate", "asof_dt", "stale_date", "stale_dt"] },
  { header: "Rule Logic",      keys: ["rule_logic", "logic", "rule"] },
  { header: "Z Score",         keys: ["z_score", "zscore", "z score"] },
  { header: "Std Value",       keys: ["std_value", "std value", "std", "stddev", "std_dev", "1.2_std_value", "1_2_std_value", "12stdvalue"] },
  { header: "Mean Value",      keys: ["mean_value", "mean value", "mean", "1.2_mean_value", "1_2_mean_value", "12meanvalue"] },
  { header: "Is Outlier",      keys: ["is_outlier", "is outlier", "outlier"] },
  { header: "Unique Tag",      keys: ["unique_tag", "unique tag", "uniquetag"] },
];

function pickKey(row, candidates) {
  if (!row) return null;
  const cand = new Set((candidates || []).map(normKey));
  for (const k of Object.keys(row)) if (cand.has(normKey(k))) return k;
  return null;
}
function findKeyInRows(rows, candidates) {
  if (!rows?.length) return null;
  const cand = new Set((candidates || []).map(normKey));
  const tally = new Map();
  for (const r of rows) {
    for (const k of Object.keys(r || {})) {
      if (cand.has(normKey(k))) tally.set(k, (tally.get(k) || 0) + 1);
    }
  }
  let best = null, max = 0;
  for (const [k, n] of tally.entries()) if (n > max) { best = k; max = n; }
  return best;
}

/* latest-date helpers */
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
    const norm = parseYMD(d);
    if (!norm) continue;
    if (!max || norm > max) max = norm;
  }
  return max;
}

/* normalize rows */
function normalizeRows(rows) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    // Map Y2021..Y2025 → 2021..2025; default to 0 for year totals
    FIXED_YEARS.forEach((y) => {
      const yPlain = Number(o[y]);
      const yPref = Number(o[`Y${y}`]);
      if (Number.isFinite(yPlain)) o[y] = yPlain;
      else if (Number.isFinite(yPref)) o[y] = yPref;
      else o[y] = 0;
      if (`Y${y}` in o) delete o[`Y${y}`];
    });

    // hidden group-by book
    const bKey = pickKey(o, ["book", "book_nm", "book name"]);
    o.__book = bKey ? o[bKey] : "";

    // backfill a date-ish field from report_date
    const reportDate = o.report_date || "";
    const dateKeys = ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"];
    const dk = pickKey(o, dateKeys);
    if (dk && isNilOrEmpty(o[dk])) o[dk] = reportDate || "—";

    // booleans
    const boolNames = ["is outlier", "is_outlier", "outlier"];
    for (const nm of boolNames) {
      const k = pickKey(o, [nm]);
      if (k != null) o[k] = o[k] ? 1 : 0;
    }

    // numeric preferred – keep null if absent
    const numericNames = [
      "z score","z_score","zscore",
      "std value","std_value","std","stddev","std_dev","1.2_std_value","1_2_std_value","12stdvalue",
      "mean value","mean_value","mean","1.2_mean_value","1_2_mean_value","12meanvalue",
    ];
    for (const nm of numericNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : null;
      }
    }

    return o;
  });
}

/* aggregators */
const aggFirst = (params) => {
  for (const v of params.values || []) if (v !== undefined && v !== null && v !== "") return v;
  return null;
};
const aggAvg = (params) => {
  const vals = (params.values || []).map(Number).filter((n) => Number.isFinite(n));
  if (!vals.length) return null;
  return vals.reduce((a, b) => a + b, 0) / vals.length;
};

/* build columns */
function buildColumnDefs(rows) {
  const preferredKeyMap = PREFERRED_FIELDS.map((pf) => ({
    header: pf.header,
    actualKey: findKeyInRows(rows, pf.keys),
    keys: pf.keys,
  }));

  const groupCols = [
    { headerName: "Report Type", field: "report_type", rowGroup: true, rowGroupIndex: 0, hide: true },
    { headerName: "Book", field: "__book", rowGroup: true, rowGroupIndex: 1, hide: true },
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

  const numericKeySet = new Set([
    "zscore","z_score","z score",
    "stdvalue","std_value","std","stddev","std_dev","1.2_std_value","1_2_std_value","12stdvalue",
    "meanvalue","mean_value","mean","1.2_mean_value","1_2_mean_value","12meanvalue",
  ].map(normKey));

  const preferredCols = preferredKeyMap.map(({ header, actualKey, keys }) => {
    const isNumeric = keys.some((k) => numericKeySet.has(normKey(k)));
    const col = {
      headerName: header,
      sortable: true,
      resizable: true,
      headerTooltip: header,
      minWidth: 160,
      filter: isNumeric ? "agNumberColumnFilter" : "agTextColumnFilter",
      aggFunc: isNumeric ? aggAvg : aggFirst,
    };
    if (actualKey) {
      col.field = actualKey;
    } else {
      col.valueGetter = (p) => {
        if (!p?.data) return null;
        const k = pickKey(p.data, keys);
        return k ? p.data[k] : null;
      };
    }
    return col;
  });

  const seen = new Set([
    "report_type", "__book", "book", "book_nm", "report_date",
    ...FIXED_YEARS,
    ...preferredKeyMap.map((x) => x.actualKey).filter(Boolean),
  ]);

  const allRowKeys = rows?.length ? Array.from(new Set(rows.flatMap((r) => Object.keys(r || {})))) : [];
  const extras = allRowKeys
    .filter((k) => !seen.has(k) && !/^Y\d{4}$/i.test(k))
    .map((k) => ({
      headerName: /^\d{4}$/.test(k) ? k : prettify(k),
      field: k,
      sortable: true,
      resizable: true,
      minWidth: 140,
      filter: /^\d{4}$/.test(k) ? "agNumberColumnFilter" : "agTextColumnFilter",
      aggFunc: aggFirst,
    }));

  return [...groupCols, reportDateCol, ...yearCols, ...preferredCols, ...extras];
}

/* tolerate multiple response shapes */
function coerceRows(json) {
  if (Array.isArray(json)) return json;
  if (json && Array.isArray(json.rows)) return json.rows;
  if (json && Array.isArray(json.details)) return json.details;
  if (json && Array.isArray(json.data)) return json.data;
  if (json && Array.isArray(json.result)) return json.result;
  return [];
}

export default function CosmosReports() {
  const methods = useForm({ defaultValues: { report_date: "" } });
  const { setValue, control } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);
  const [exportFmt, setExportFmt] = useState("csv");

  const gridRef = useRef(null);
  const fetchedOnce = useRef(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);

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
      const data = coerceRows(json);
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

  // ---- Export helpers ----
  function toCSV(rows, cols, sep) {
    const headers = cols.map((c) => c.headerName);
    const fields = cols.map((c) => c.field || c.colId);
    const lines = [headers.join(sep)];
    for (const r of rows) {
      const line = fields.map((f) => {
        let v = f ? r[f] : null;
        if (v === undefined || v === null) v = "";
        const s = String(v).replace(/"/g, '""');
        return /[,"\n\t]/.test(s) ? `"${s}"` : s;
      });
      lines.push(line.join(sep));
    }
    return lines.join("\n");
  }
  function download(name, mime, text) {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }
  function doExport() {
    const api = gridRef.current?.api;
    const columnApi = gridRef.current?.columnApi;
    if (!api || !columnApi) return;

    const rowsOut = [];
    api.forEachNodeAfterFilterAndSort((n) => {
      if (!n.group && n.data) rowsOut.push(n.data);
    });

    const visCols = columnApi.getAllDisplayedColumns()
      .filter((c) => !c.isRowGroupActive())
      .map((c) => ({
        headerName: c.getColDef().headerName,
        field: c.getColDef().field,
        colId: c.getColId(),
      }));

    if (exportFmt === "json") {
      download("dq_reports.json", "application/json;charset=utf-8", JSON.stringify(rowsOut, null, 2));
    } else if (exportFmt === "tsv") {
      download("dq_reports.tsv", "text/tab-separated-values;charset=utf-8", toCSV(rowsOut, visCols, "\t"));
    } else {
      download("dq_reports.csv", "text/csv;charset=utf-8", toCSV(rowsOut, visCols, ","));
    }
  }
  // -----------------------

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

                {/* Export */}
                <div className="flex items-center gap-2">
                  <select
                    value={exportFmt}
                    onChange={(e) => setExportFmt(e.target.value)}
                    style={{
                      border: "1px solid #ccc",
                      borderRadius: "4px",
                      padding: "4px 8px",
                      fontSize: "14px",
                    }}
                  >
                    <option value="csv">CSV</option>
                    <option value="tsv">TSV</option>
                    <option value="json">JSON</option>
                  </select>
                  <Button size="sm" variant="outline" onClick={doExport}>Export</Button>
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
