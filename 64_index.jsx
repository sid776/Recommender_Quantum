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
const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v == null || v === "";

const PREFERRED_FIELDS = [
  { header: "Rule Type",       keys: ["rule_type", "ruletype"] },
  { header: "Risk Factor Id",  keys: ["risk_factor_id", "risk factor id", "rf_id", "riskfactorid"] },
  { header: "Stale Date",      keys: ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"] },
  { header: "Rule Logic",      keys: ["rule_logic", "logic", "rule"] },
  { header: "Z Score",         keys: ["z_score", "zscore", "z score"] },
  { header: "Std Value",       keys: ["std_value", "std value", "std", "stddev", "std_dev"] },
  { header: "Mean Value",      keys: ["mean_value", "mean value", "mean"] },
  { header: "Is Outlier",      keys: ["is_outlier", "is outlier", "outlier"] },
  { header: "Unique Tag",      keys: ["unique_tag", "unique tag", "uniquetag"] },
];

function pickKey(row, candidates) {
  if (!row) return null;
  const candSet = new Set(candidates.map(lc));
  for (const k of Object.keys(row)) if (candSet.has(lc(k))) return k;
  return null;
}
function getFromAny(row, keys) {
  if (!row) return undefined;
  for (const k of keys) {
    const found = Object.prototype.hasOwnProperty.call(row, k) ? k : pickKey(row, [k]);
    if (found && row[found] !== undefined) return row[found];
  }
  return undefined;
}

/* latest date helpers */
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

/* Normalize rows (and derive a hidden __book for grouping) */
function normalizeRows(rows) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    // map Y2021..Y2025 -> 2021..2025, keep 0 if missing
    FIXED_YEARS.forEach((y) => {
      const yPref = `Y${y}`;
      const plain = Number(o[y]);
      const fromY = Number(o[yPref]);
      if (Number.isFinite(plain)) o[y] = plain;
      else if (Number.isFinite(fromY)) o[y] = fromY;
      else o[y] = 0;
      if (yPref in o) delete o[yPref];
    });

    // derive __book for grouping (don't show as a standalone column)
    const bKey = pickKey(o, ["book", "book_nm", "book name"]);
    o.__book = bKey ? o[bKey] : "";

    // fill a date-ish field from report_date if needed
    const reportDate = o.report_date || "";
    const dateKeys = ["stale_date", "stale dt", "stale_dt", "as_of_date", "as_of_dt", "asofdate", "asof_dt"];
    const dk = pickKey(o, dateKeys);
    if (dk && isNilOrEmpty(o[dk])) o[dk] = reportDate || "—";

    // numeric cleanups
    const numericNames = [
      "mean_value","mean value","mean",
      "z score","z_score","zscore",
      "std value","std_value","std","stddev","std_dev",
      "risk_factor_value","rf_id","value",
    ];
    for (const nm of numericNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : (o[k] ?? 0);
      }
    }

    // booleans
    const boolNames = ["is outlier","is_outlier","outlier"];
    for (const nm of boolNames) {
      const k = pickKey(o, [nm]);
      if (k) o[k] = o[k] ? 1 : 0;
    }

    return o;
  });
}

/* Columns: group by Report Type → Book, then Report Date, Years, Preferred, Extras */
function buildColumnDefs(rows) {
  const baseGroupCols = [
    {
      headerName: "Report Type",
      field: "report_type",
      rowGroup: true,
      rowGroupIndex: 0,
      hide: true,
    },
    {
      headerName: "Book",
      field: "__book",
      rowGroup: true,
      rowGroupIndex: 1,
      hide: true,
    },
  ];

  const reportDateCol = {
    headerName: "Report Date",
    field: "report_date",
    sortable: true,
    resizable: true,
    minWidth: 150,
    filter: "agTextColumnFilter",
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

  const preferredCols = PREFERRED_FIELDS.map((pf) => ({
    headerName: pf.header,
    field: pf.keys[0],
    valueGetter: (p) => getFromAny(p?.data, pf.keys),
    sortable: true,
    resizable: true,
    headerTooltip: pf.header,
    filter: "agTextColumnFilter",
    minWidth: 160,
  }));

  // extras after preferred
  const seen = new Set([
    "report_type",
    "__book",
    "book",
    "book_nm",
    "report_date",
    ...PREFERRED_FIELDS.flatMap((p) => p.keys),
    ...FIXED_YEARS,
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
    }));

  return [...baseGroupCols, reportDateCol, ...yearCols, ...preferredCols, ...extras];
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
      const url = dateStr
        ? `${API_ENDPOINT}?report_date=${encodeURIComponent(dateStr)}&limit=500`
        : `${API_ENDPOINT}?limit=500`;
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) { setRows([]); return; }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);

      // default the COB field to the latest date present in payload
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
              style={{ width: "100%", border: "none", ["--ag-borders"]: "none", ["--ag-border-color"]: "transparent" }}
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
