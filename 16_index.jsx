// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const MIN_YEAR = 2021;

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v === null || v === undefined || v === "";

const DETAIL_FIELDS = [
  { header: "Unique Tag", keys: ["unique_tag", "unique tag", "uniquetag"] },
  { header: "Risk Factor Value", keys: ["risk_factor_value", "risk factor value", "rf_value", "value"] },
  { header: "Mean Value", keys: ["mean_value", "mean value", "mean"] },
  { header: "Z Score", keys: ["z score", "z_score", "zscore"] },
  { header: "Std Value", keys: ["std value", "std_value", "std", "stddev", "std_dev"] },
  { header: "Is Outlier", keys: ["is outlier", "is_outlier", "outlier"] },
];

const ERROR_KEYS = [
  "error_source", "error source", "source",
  "error_details", "error details", "details",
  "error_msg", "error message", "message",
];

const lc = (s) => String(s || "").toLowerCase();

function pickKey(row, candidates) {
  if (!row) return null;
  const candSet = new Set(candidates.map(lc));
  for (const k of Object.keys(row)) if (candSet.has(lc(k))) return k;
  return null;
}
function findKeyInRows(rows, candidates) {
  if (!rows?.length) return null;
  for (const r of rows) {
    const k = pickKey(r, candidates);
    if (k) return k;
  }
  return null;
}

function getYearKeys(rows) {
  if (!rows?.length) return ["2021", "2022", "2023", "2024", "2025"];
  const years = Array.from(
    new Set(
      rows.flatMap((r) =>
        Object.keys(r || {}).filter((k) => /^\d{4}$/.test(k) && Number(k) >= MIN_YEAR)
      )
    )
  ).sort();
  return years.length ? years : ["2021", "2022", "2023", "2024", "2025"];
}

function normalizeRows(rows, YEARS) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    YEARS.forEach((y) => {
      const n = Number(o[y]);
      o[y] = Number.isFinite(n) ? n : 0;
    });

    const reportDate = o.report_date || "";
    const dateKeys = ["as_of_date", "as_of_dt", "asofdate", "asof_dt"];
    const dk = pickKey(o, dateKeys);
    if (dk && isNilOrEmpty(o[dk])) {
      o[dk] = reportDate || "—";
    }

    const numericNames = [
      "mean_value","mean value","mean",
      "z score","z_score","zscore",
      "std value","std_value","std","stddev","std_dev",
      "risk_factor_value","rf_value","value",
    ];
    for (const nm of numericNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : (o[k] === null || o[k] === undefined ? 0 : o[k]);
      }
    }

    const boolNames = ["is outlier", "is_outlier", "outlier"];
    for (const nm of boolNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        if (!Number.isFinite(n)) o[k] = o[k] ? 1 : 0;
      }
    }

    return o;
  });
}

/** Decide if a column is numeric-ish based on name */
function isNumericHeader(headerName, field) {
  const name = lc(headerName);
  return (
    ["z score", "std value", "mean value", "risk factor value"].includes(name) ||
    /^\d{4}$/.test(field)
  );
}

/** Build col defs; attach tooltips to first 4–5 numeric-ish columns; make numeric cells clickable */
function buildColumnDefs(rows, YEARS) {
  const base = ["report_date", "risk_factor_id", "rule_type", "book"];

  const detailMap = DETAIL_FIELDS.map((df) => {
    const k = findKeyInRows(rows, df.keys);
    return { header: df.header, key: k };
  }).filter((d) => !!d.key);

  const ordered = [...base, ...YEARS, ...detailMap.map((d) => d.key)];
  const seen = new Set(ordered);

  const allRowKeys = rows?.length
    ? Array.from(new Set(rows.flatMap((r) => Object.keys(r || {}))))
    : [];

  const extras = allRowKeys.filter((k) => {
    if (seen.has(k)) return false;
    if (/^\d{4}$/.test(k) && Number(k) < MIN_YEAR) return false;
    return true;
  });

  const allKeys = [...ordered, ...extras];

  // Cache error keys found anywhere for tooltips
  const errKeyGlobal = findKeyInRows(rows, ERROR_KEYS);

  // Renderer: underline numeric cells to hint "clickable"
  const clickableRenderer = (params) => {
    const el = document.createElement("span");
    const v = params.value;
    const looksNumeric = typeof v === "number" && Number.isFinite(v);
    el.textContent = v == null ? "" : String(v);
    if (looksNumeric) {
      el.style.cursor = "pointer";
      el.style.textDecoration = "underline";
      el.style.textUnderlineOffset = "2px";
    }
    return el;
  };

  // Count numeric-ish columns to flag first 5 for richer tooltip
  let numericSeen = 0;

  return allKeys.map((k) => {
    const isYear = YEARS.includes(k);
    const detail = detailMap.find((d) => d.key === k);
    const headerName = detail ? detail.header : prettify(k);

    const numericLike = isNumericHeader(headerName, k);
    const dateLike = lc(headerName).includes("date") || lc(k).endsWith("_dt");

    // We’ll attach a richer tooltip to the first five numeric-like columns encountered
    let attachRichTooltip = false;
    if (numericLike && numericSeen < 5) {
      attachRichTooltip = true;
      numericSeen += 1;
    }

    const col = {
      headerName,
      field: k,
      sortable: true,
      resizable: true,
      headerTooltip: headerName,
      suppressHeaderMenuButton: false,
      filter: numericLike ? "agNumberColumnFilter" : dateLike ? "agDateColumnFilter" : "agTextColumnFilter",
      minWidth: isYear ? 110 : 160,
      // tooltip per cell when flagged
      tooltipValueGetter: attachRichTooltip
        ? (p) => {
            if (!p || !p.data) return null;
            const rf = p.data.risk_factor_id ?? "—";
            const rt = p.data.rule_type ?? "—";
            const bk = p.data.book ?? "—";
            const rd = p.data.report_date ?? "—";
            const val = p.value ?? "—";
            const errK = errKeyGlobal || pickKey(p.data, ERROR_KEYS);
            const errV = errK ? p.data[errK] : null;
            const errLine = errV ? `\nDetails: ${String(errV).slice(0, 200)}` : "";
            return `Date: ${rd}\nRF: ${rf}\nRule: ${rt}\nBook: ${bk}\nCol: ${headerName}\nVal: ${val}${errLine}`;
          }
        : undefined,
      // renderer: make numeric cells look clickable
      cellRenderer: numericLike ? clickableRenderer : undefined,
    };

    if (k === "rule_type" || k === "book") {
      col.rowGroup = true;
      col.hide = true;
    }
    if (isYear) {
      col.type = "numericColumn";
      col.aggFunc = "sum";
      col.valueParser = (p) => Number(p.newValue ?? 0);
    }
    if (detail) {
      col.valueGetter = (params) => {
        if (!params || !params.node) return null;
        if (params.node.group) return null;
        return params.data ? params.data[k] : null;
      };
      col.aggFunc = null;
      col.suppressAggFuncInHeader = true;
    }

    return col;
  });
}

/** Build a “far-right columns” list to show on the detail page */
function getFarRightColumns(sampleRow, YEARS) {
  if (!sampleRow) return [];
  const base = new Set(["report_date", "risk_factor_id", "rule_type", "book", ...YEARS]);
  const keys = Object.keys(sampleRow);
  // heuristics: skip base; pick the last 8 columns (often far-right in the grid)
  const extra = keys.filter((k) => !base.has(k));
  return extra.slice(-8);
}

/** Open a simple new tab with dummy metrics + some far-right columns from the row */
function openDetailPage(row, colId, value, farRightCols) {
  const rf = row?.risk_factor_id ?? "—";
  const rt = row?.rule_type ?? "—";
  const bk = row?.book ?? "—";
  const rd = row?.report_date ?? "—";

  // Dummy metrics
  const details = [
    { label: "Metric", value: colId },
    { label: "Cell Value", value: value ?? "—" },
    { label: "Mean Value", value: (Math.random() * 1.5 + 0.5).toFixed(6) },
    { label: "Std Dev", value: (Math.random() * 0.8 + 0.1).toFixed(6) },
    { label: "Z Score", value: (Math.random() * 4 - 2).toFixed(3) },
    { label: "Is Outlier", value: Math.random() > 0.7 ? "Yes" : "No" },
  ];

  const farRows = (farRightCols || []).map((k) => ({
    field: prettify(k),
    value: row?.[k] ?? "—",
  }));

  const html = `
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>DQ Detail</title>
        <style>
          body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }
          .card { max-width: 1000px; margin: 0 auto; border: 1px solid #e5e7eb; border-radius: 16px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
          h1 { font-size: 20px; margin: 0 0 12px; }
          .meta { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 8px; margin-bottom: 16px; }
          .pill { background: #f3f4f6; border-radius: 999px; padding: 6px 10px; font-size: 12px; color: #374151; }
          .grid { display: grid; gap: 16px; grid-template-columns: 1fr 1fr; }
          table { width: 100%; border-collapse: collapse; }
          th, td { border-bottom: 1px solid #e5e7eb; text-align: left; padding: 10px; font-size: 14px; }
          th { background: #f9fafb; color: #111827; }
          .muted { color: #6b7280; }
          .link { color: #2563eb; text-decoration: none; }
          .link:hover { text-decoration: underline; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>Data Quality • Cell Detail</h1>
          <div class="meta">
            <div class="pill">Date: <strong>${rd}</strong></div>
            <div class="pill">Risk Factor: <strong>${rf}</strong></div>
            <div class="pill">Rule: <strong>${rt}</strong></div>
            <div class="pill">Book: <strong>${bk}</strong></div>
          </div>

          <div class="grid">
            <div>
              <table>
                <thead><tr><th>Field</th><th>Value</th></tr></thead>
                <tbody>
                  ${details.map((d) => `<tr><td class="muted">${d.label}</td><td>${d.value}</td></tr>`).join("")}
                </tbody>
              </table>
              <p class="muted" style="margin-top:10px">(Dummy metrics for demo)</p>
            </div>
            <div>
              <table>
                <thead><tr><th>Column</th><th>Value</th></tr></thead>
                <tbody>
                  ${farRows.map((r) => `<tr><td class="muted">${r.field}</td><td>${String(r.value)}</td></tr>`).join("")}
                </tbody>
              </table>
              <p class="muted" style="margin-top:10px">(Showing a few far-right columns from the same row)</p>
            </div>
          </div>

          <p style="margin-top:14px"><a class="link" href="javascript:window.close()">Close tab</a></p>
        </div>
      </body>
    </html>
  `.trim();

  const w = window.open("/dq/detail", "_blank", "noopener,noreferrer");
  if (w && w.document) {
    w.document.open();
    w.document.write(html);
    w.document.close();
  }
}

export default function CosmosReports() {
  const methods = useForm({ defaultValues: { report_date: "" } });
  const { setValue, control } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);

  const gridRef = useRef(null);
  const fetchedOnce = useRef(false);

  const YEARS = useMemo(() => getYearKeys(rows), [rows]);
  const normRows = useMemo(() => normalizeRows(rows, YEARS), [rows, YEARS]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows, YEARS), [normRows, YEARS]);

  // Precompute far-right columns from the first row to use in the detail page
  const farRightCols = useMemo(() => getFarRightColumns(normRows?.[0] || null, YEARS), [normRows, YEARS]);

  const reportDate = useWatch({ control, name: "report_date" });

  async function fetchData(dateStr) {
    setLoading(true);
    try {
      const url =
        dateStr && dateStr.length
          ? `${API_ENDPOINT}?report_date=${encodeURIComponent(dateStr)}&limit=500`
          : `${API_ENDPOINT}?limit=500`;
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) {
        setRows([]);
        return;
      }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);

      if ((!dateStr || !dateStr.length) && data?.length) {
        const raw = data[0]?.report_date ?? "";
        let normalized = "";
        if (typeof raw === "string" && /^\d{8}$/.test(raw)) {
          normalized = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
        } else if (typeof raw === "string" && /^\d{4}-\d{2}-\d{2}$/.test(raw)) {
          normalized = raw;
        } else if (raw instanceof Date) {
          normalized = raw.toISOString().slice(0, 10);
        }
        if (normalized) setValue("report_date", normalized, { shouldDirty: false });
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

  // Click any numeric cell → open dummy detail page
  const onCellClicked = (ev) => {
    if (!ev || ev.node?.group) return;
    const value = ev.value;
    const isNumeric = typeof value === "number" && Number.isFinite(value);
    if (!isNumeric) return;
    const colId = ev.colDef?.field || ev.column?.getColId() || "value";
    openDetailPage(ev.data, colId, value, farRightCols);
  };

  return (
    <Box className="overflow-hidden" height="calc(100vh - 70px)">
      {/* Center the side buttons vertically */}
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
                  minWidth: 110,
                  sortable: true,
                  filter: true,
                  resizable: true,
                  enableValue: true,
                  enableRowGroup: true,
                  enablePivot: true,
                  enableCharts: true,
                  floatingFilter: showFloatingFilters,
                  // enable tooltips globally; we feed custom strings per col
                  tooltipValueGetter: (p) => p?.colDef?.tooltipValueGetter ? p.colDef.tooltipValueGetter(p) : null,
                }}
                autoGroupColumnDef={{
                  headerName: "Group",
                  minWidth: 260,
                  pinned: "left",
                }}
                headerHeight={42}
                floatingFiltersHeight={36}
                loading={loading}
                animateRows={true}
                enableRangeSelection={true}
                suppressAggFuncInHeader={true}
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
                }}
                onCellClicked={onCellClicked}
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
