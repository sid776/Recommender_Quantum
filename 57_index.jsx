// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const lc = (s) => String(s || "").toLowerCase();
const isNilOrEmpty = (v) => v === null || v === undefined || v === "";

/* tolerate naming variants for the columns you want */
const PREFERRED_FIELDS = [
  { header: "Rule Type",        keys: ["rule_type", "ruletype"] },
  { header: "Risk Factor Id",   keys: ["risk_factor_id", "risk factor id", "rf_id", "riskfactorid"] },
  { header: "Stale Date",       keys: ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"] },
  { header: "Rule Logic",       keys: ["rule_logic", "logic", "rule"] },
  { header: "Year",             keys: ["year"] }, // single “year” column if present; not Y2021… etc.
  { header: "Z Score",          keys: ["z_score", "zscore", "z score"] },
  { header: "Std Value",        keys: ["std_value", "std value", "std", "stddev", "std_dev"] },
  { header: "Mean Value",       keys: ["mean_value", "mean value", "mean"] },
  { header: "Is Outlier",       keys: ["is_outlier", "is outlier", "outlier"] },
  { header: "Unique Tag",       keys: ["unique_tag", "unique tag", "uniquetag"] },
];

/* helpers */
function pickKey(row, candidates) {
  if (!row) return null;
  const candSet = new Set(candidates.map((x) => String(x).toLowerCase()));
  for (const k of Object.keys(row)) if (candSet.has(String(k).toLowerCase())) return k;
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

/* latest date detection */
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
    if (!max || norm > max) max = norm; // lexical ok for YYYY-MM-DD
  }
  return max;
}

/* normalize rows (numbers/bools and backfill stale/as_of with report_date) */
function normalizeRows(rows) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    // numeric coercions for known numeric columns
    const numericNames = [
      "mean_value", "mean value", "mean",
      "z score", "z_score", "zscore",
      "std value", "std_value", "std", "stddev", "std_dev",
      "risk_factor_value", "rf_value", "value",
    ];
    for (const nm of numericNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : (o[k] === null || o[k] === undefined ? 0 : o[k]);
      }
    }
    // boolean coercion
    const boolNames = ["is outlier", "is_outlier", "outlier"];
    for (const nm of boolNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        if (!Number.isFinite(n)) o[k] = o[k] ? 1 : 0;
      }
    }
    // backfill stale/as_of date from report_date if blank
    const reportDate = o.report_date || "";
    const dateKeys = ["stale_date", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"];
    const dk = pickKey(o, dateKeys);
    if (dk && isNilOrEmpty(o[dk])) o[dk] = reportDate || "—";

    return o;
  });
}

/* only build columns you want, hide years/Y#### */
function buildColumnDefs(rows) {
  // detect data-backed keys for preferred fields
  const preferred = PREFERRED_FIELDS.map((pf) => {
    const k = findKeyInRows(rows, pf.keys);
    return k ? { header: pf.header, key: k } : null;
  }).filter(Boolean);

  // base + preferred (report_type first so we can group by it)
  const orderedKeys = ["report_type", "report_date", ...preferred.map((d) => d.key)];

  // extras: anything not already included, excluding years and Y#### noise
  const seen = new Set(orderedKeys);
  const allRowKeys = rows?.length ? Array.from(new Set(rows.flatMap((r) => Object.keys(r || {})))) : [];
  const extras = allRowKeys.filter((k) => {
    if (seen.has(k)) return false;
    if (/^\d{4}$/.test(k)) return false;   // 2021..2025
    if (/^Y\d{4}$/i.test(k)) return false; // Y2021..Y2025
    return true;
  });

  const allKeys = [...orderedKeys, ...extras];

  return allKeys.map((k) => {
    const pf = preferred.find((d) => d.key === k);
    const headerName = pf ? pf.header : prettify(k);
    const isLikelyDate = lc(headerName).includes("date") || lc(k).endsWith("_dt");

    const col = {
      headerName,
      field: k,
      sortable: true,
      resizable: true,
      headerTooltip: headerName,
      filter: isLikelyDate ? "agDateColumnFilter" : "agTextColumnFilter",
      minWidth: 150,
    };

    if (k === "report_type") { col.rowGroup = true; col.hide = true; }

    return col;
  });
}

export default function CosmosReports() {
  const methods = useForm({ defaultValues: { report_date: "" } });
  const { setValue, control } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);

  const gridRef = useRef(null);
  const fetchedOnce = useRef(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);

  const reportDate = useWatch({ control, name: "report_date" });

  async function fetchData(dateStr) {
    setLoading(true);
    try {
      const url =
        dateStr && dateStr.length
          ? `${API_ENDPOINT}?report_date=${encodeURIComponent(dateStr)}&limit=500`
          : `${API_ENDPOINT}?limit=500`;
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) { setRows([]); return; }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);

      // COB = latest date present in payload
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
    api.sizeColumnsToFit({ defaultMinWidth: 120 });
    api.setGridOption("suppressAggFuncInHeader", true);
  };

  return (
    <Box className="overflow-hidden" height="calc(100vh - 70px)">
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
              className="ag-theme-alpine rounded-lg flex-1"
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
                animateRows={true}
                enableRangeSelection={true}
                suppressAggFuncInHeader={true}
                onFirstDataRendered={onFirstDataRendered}
                suppressHorizontalScroll={false}
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
