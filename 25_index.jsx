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

/* ---------- ONLY CHANGE: make date parsing accept ISO + common variants ---------- */
function parseYMD(anyVal) {
  if (anyVal === null || anyVal === undefined) return null;

  // Date object
  if (anyVal instanceof Date && !isNaN(anyVal)) {
    return anyVal.toISOString().slice(0, 10);
  }
  const s = String(anyVal).trim().replace(/['"]/g, "");

  // ISO with or without time: "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS"
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;

  // Compact "YYYYMMDD"
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;

  // Slash variant "YYYY/MM/DD"
  const ms = s.match(/^(\d{4})\/(\d{2})\/(\d{2})/);
  if (ms) return `${ms[1]}-${ms[2]}-${ms[3]}`;

  return null;
}
function maxDateStr(dates) {
  let max = null;
  for (const d of dates) {
    const norm = parseYMD(d);
    if (!norm) continue;
    if (!max || norm > max) max = norm; // lexical compare works with YYYY-MM-DD
  }
  return max;
}
/* ------------------------------------------------------------------------------- */

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
      "mean_value",
      "mean value",
      "mean",
      "z score",
      "z_score",
      "zscore",
      "std value",
      "std_value",
      "std",
      "stddev",
      "std_dev",
      "risk_factor_value",
      "rf_value",
      "value",
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

  return allKeys.map((k) => {
    const isYear = YEARS.includes(k);
    const detail = detailMap.find((d) => d.key === k);
    const isDetail = !!detail;
    const headerName = isDetail ? detail.header : prettify(k);

    const isLikelyNumber =
      isYear ||
      ["z score", "std value", "mean value", "risk factor value"].includes(lc(headerName)) ||
      /^\d{4}$/.test(k);

    const isLikelyDate = lc(headerName).includes("date") || lc(k).endsWith("_dt");

    const col = {
      headerName,
      field: k,
      sortable: true,
      resizable: true,
      headerTooltip: headerName,
      suppressHeaderMenuButton: false,
      filter: isLikelyNumber ? "agNumberColumnFilter" : isLikelyDate ? "agDateColumnFilter" : "agTextColumnFilter",
      minWidth: isYear ? 110 : 160,
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

    if (isDetail) {
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

      // If user didn’t pick a date, auto-lock to the latest present in the payload
      if ((!dateStr || !dateStr.length) && data?.length) {
        const allDates = data.map((r) => r?.report_date).filter(Boolean);
        const latest = maxDateStr(allDates);
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
          margin-top: 24px;
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
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
