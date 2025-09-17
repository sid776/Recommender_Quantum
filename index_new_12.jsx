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
  const [open, setOpen] = useState(true);
  const [loading, setLoading] = useState(false);
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);

  const gridRef = useRef(null);
  const fetchedOnce = useRef(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

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

      // seed latest date into the date field if empty
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
    setOpen(true);
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

    // Auto-size headers for clean, bold look; then fill remaining width
    const ids = [];
    columnApi.getColumns()?.forEach((c) => ids.push(c.getColId()));
    columnApi.autoSizeColumns(ids, true);
    api.sizeColumnsToFit({ defaultMinWidth: 110 });
    api.setGridOption("suppressAggFuncInHeader", true);
  };

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            {/* Top bar like your reference (title left, COB + filter icon right) */}
            <div className="flex items-center justify-between">
              <div className="text-lg font-bold">DQ Reports</div>
              <div className="flex items-end gap-3">
                <div className="w-[240px]">
                  <InputFieldset
                    id="report_date"
                    label="COB"
                    fieldName="report_date"
                    tooltipMsg="Report Date"
                    type="date"
                  />
                </div>
                <button
                  type="button"
                  title="Toggle Filters"
                  className={`h-9 w-9 rounded-md border flex items-center justify-center ${
                    showFloatingFilters ? "border-[#004832] text-[#004832]" : "border-gray-300 text-gray-700"
                  }`}
                  onClick={() => setShowFloatingFilters((v) => !v)}
                >
                  <i className="ph ph-funnel-simple" />
                </button>
              </div>
            </div>

            {/* Section header row */}
            <div className="mt-3 border rounded-md">
              <div className="flex items-center justify-between px-3 py-2">
                <div className="flex items-center gap-2">
                  <i
                    className={`ph ${open ? "ph-caret-down" : "ph-caret-right"} cursor-pointer`}
                    onClick={() => setOpen((v) => !v)}
                  />
                  <span className="font-semibold">Combined DQ Reports</span>
                  <span className="text-xs text-gray-600">({normRows.length})</span>
                </div>
              </div>

              {open && (
                <Box className="ag-theme-alpine rounded-b-lg" style={{ height: "600px", width: "100%" }}>
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
                    sideBar={{
                      position: "right",
                      defaultToolPanel: null,
                      toolPanels: [
                        {
                          id: "columns",
                          labelDefault: "Columns",
                          iconKey: "columns",
                          toolPanel: "agColumnsToolPanel",
                          toolPanelParams: { suppressPivotMode: true },
                        },
                        {
                          id: "filters",
                          labelDefault: "Filter",
                          iconKey: "filter",
                          toolPanel: "agFiltersToolPanel",
                        },
                      ],
                    }}
                    headerHeight={42}
                    floatingFiltersHeight={36}
                    loading={loading}
                    animateRows={true}
                    enableRangeSelection={true}
                    suppressAggFuncInHeader={true}
                    onFirstDataRendered={onFirstDataRendered}
                    getContextMenuItems={(params) => {
                      const def = params.defaultItems;
                      const custom = ["chartRange", "pivotChart"];
                      return [...def, "separator", ...custom];
                    }}
                  />
                </Box>
              )}
            </div>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
