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

// canonical detail fields (header, candidate keys to read from)
const DETAIL_FIELDS = [
  { header: "Unique Tag", keys: ["unique_tag", "unique tag", "uniquetag"] },
  { header: "Risk Factor Value", keys: ["risk_factor_value", "risk factor value", "rf_value", "value"] },
  { header: "Mean Value", keys: ["mean_value", "mean value", "mean"] },
  { header: "Z Score", keys: ["z score", "z_score", "zscore"] },
  { header: "Std Value", keys: ["std value", "std_value", "std"] },
  { header: "Is Outlier", keys: ["is outlier", "is_outlier", "outlier"] },
];

// find a column in a row by any of the candidate keys
function pickKey(row, candidates) {
  if (!row) return null;
  for (const k of Object.keys(row)) {
    if (candidates.includes(lc(k))) return k;
  }
  return null;
}

function getYearKeys(rows) {
  if (!rows?.length) return ["2021", "2022", "2023", "2024", "2025"];
  const years = Object.keys(rows[0] || {})
    .filter((k) => /^\d{4}$/.test(k) && Number(k) >= 2021) // keep >= 2021
    .sort();
  return years.length ? years : ["2021", "2022", "2023", "2024", "2025"];
}

function normalizeRows(rows, YEARS) {
  return (rows || []).map((r) => {
    const o = { ...(r || {}) };

    // ensure year values are numeric
    YEARS.forEach((y) => {
      const n = Number(o[y]);
      o[y] = Number.isFinite(n) ? n : 0;
    });

    // backfill detail fields with reasonable defaults (only for leaf display later)
    // We don't rename the source keys; we just ensure values exist if the keys exist.
    // Rendering will read by discovered key names.
    const reportDate = o.report_date || "";
    const dateKeys = ["as_of_date", "as_of_dt", "asofdate", "asof_dt"];
    const dk = pickKey(o, dateKeys);
    if (dk && (o[dk] === null || o[dk] === undefined || o[dk] === "")) {
      o[dk] = reportDate || "—";
    }

    // numeric-ish fields default to 0 if missing/NaN
    const numericNames = DETAIL_FIELDS
      .filter((f) => ["Z Score", "Std Value", "Mean Value"].includes(f.header))
      .flatMap((f) => f.keys);
    for (const nm of numericNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        o[k] = Number.isFinite(n) ? n : 0;
      }
    }
    const boolNames = DETAIL_FIELDS
      .filter((f) => f.header === "Is Outlier")
      .flatMap((f) => f.keys);
    for (const nm of boolNames) {
      const k = pickKey(o, [nm]);
      if (k) {
        const n = Number(o[k]);
        if (!Number.isFinite(n)) {
          o[k] = o[k] ? 1 : 0;
        }
      }
    }

    return o;
  });
}

function buildColumnDefs(rows, YEARS) {
  // base identifiers (always first)
  const base = ["report_date", "risk_factor_id", "rule_type", "book"];

  // discover actual keys for each detail field in current data
  const sample = rows?.[0] || {};
  const detailMap = DETAIL_FIELDS.map((df) => {
    const k = pickKey(sample, df.keys);
    return { header: df.header, key: k }; // key may be null if column absent
  }).filter((d) => !!d.key);

  const ordered = [...base, ...YEARS, ...detailMap.map((d) => d.key)];
  const seen = new Set(ordered);
  const extras = rows?.length ? Object.keys(rows[0]).filter((k) => !seen.has(k)) : [];
  const allKeys = [...ordered, ...extras];

  return allKeys.map((k) => {
    const isYear = YEARS.includes(k);
    const isDetail = detailMap.some((d) => d.key === k);
    const headerName = isDetail ? detailMap.find((d) => d.key === k)?.header || prettify(k) : prettify(k);

    const col = {
      headerName,
      field: k,
      sortable: true,
      resizable: true,
      headerTooltip: headerName,
      suppressHeaderMenuButton: false,
      filter:
        isYear ||
        ["z score", "std value", "mean value"].includes(lc(headerName)) ||
        /^\d{4}$/.test(k)
          ? "agNumberColumnFilter"
          : lc(headerName).includes("date")
          ? "agDateColumnFilter"
          : "agTextColumnFilter",
      minWidth: isYear ? 110 : 170,
    };

    // group by rule/book only
    if (k === "rule_type" || k === "book") {
      col.rowGroup = true;
      col.hide = true;
    }

    // year columns aggregate on parents
    if (isYear) {
      col.type = "numericColumn";
      col.aggFunc = "sum";
      col.valueParser = (p) => Number(p.newValue ?? 0);
    }

    // detail fields: show value only on leaf rows (blank on group rows)
    if (isDetail) {
      col.valueGetter = (params) => {
        if (!params || !params.node) return null;
        if (params.node.group) return null;           // parent rows blank
        return params.data ? params.data[k] : null;   // leaf rows show value
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
                suppressHorizontalScroll={false}
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
