// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const YEARS = ["2021", "2022", "2023", "2024", "2025"];

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v === null || v === undefined || v === "";

const columnIsVisible = (rows, key) => {
  if (!rows?.length) return false;
  let allEmpty = true;
  let allZero = true;
  for (const r of rows) {
    const v = r?.[key];
    if (!isNilOrEmpty(v)) allEmpty = false;
    if (Number(v || 0) !== 0) allZero = false;
    if (!allEmpty && !allZero) break;
  }
  if (YEARS.includes(key)) return !(allEmpty || allZero);
  return !allEmpty;
};

function normalizeRows(rows) {
  const base = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  return (rows || []).map((r) => {
    const o = { ...r };
    base.forEach((k) => {
      if (!(k in o)) o[k] = YEARS.includes(k) ? 0 : "";
    });
    YEARS.forEach((y) => (o[y] = Number(o[y] || 0)));
    return o;
  });
}

function buildColumnDefs(rows) {
  const ordered = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  const seen = new Set(ordered);
  const extras = rows?.length ? Object.keys(rows[0]).filter((k) => !seen.has(k)) : [];
  const allKeys = [...ordered, ...extras].filter((k) => columnIsVisible(rows, k));

  return allKeys.map((k) => {
    const isYear = YEARS.includes(k);
    const col = {
      headerName: prettify(k),
      field: k,
      sortable: true,
      filter: isYear ? "agNumberColumnFilter" : "agTextColumnFilter",
      resizable: true,
      minWidth: isYear ? 110 : 170,
      headerTooltip: prettify(k),
      suppressHeaderMenuButton: false,
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
    <Box className="overflow-x-hidden overflow-y-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3 h-full">
          <div className="p-4 bg-white shadow-md rounded-lg flex flex-col h-full">
            {/* Top bar */}
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

            {/* Grid */}
            <Box
              className="ag-theme-alpine rounded-lg flex-1"
              style={{
                width: "100%",
                border: "none",
                ["--ag-borders"]: "none",
                ["--ag-border-color"]: "transparent",
                overflowX: "hidden", // keep page from adding a second horizontal bar
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
                suppressHorizontalScroll={true}
                onFirstDataRendered={onFirstDataRendered}
              />
            </Box>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
