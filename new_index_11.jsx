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

// hide if all values are null/empty; for year columns also hide if all 0
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
  if (YEARS.includes(key)) {
    return !(allEmpty || allZero);
  }
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
    const col = {
      headerName: prettify(k),
      field: k,
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: YEARS.includes(k) ? 90 : 140,
    };
    if (k === "rule_type" || k === "book") {
      col.rowGroup = true;
      col.hide = true;
    }
    if (YEARS.includes(k)) {
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
  const fetchedOnce = useRef(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

  const reportDate = useWatch({ control, name: "report_date" });

  // fetch helper
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

      // if date was empty, set it to the latest present in data (YYYY-MM-DD)
      if ((!dateStr || !dateStr.length) && data?.length) {
        // try to read report_date; accept yyyyMMdd or yyyy-MM-dd
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

  // initial load -> pull latest and expand
  useEffect(() => {
    if (fetchedOnce.current) return;
    fetchedOnce.current = true;
    setOpen(true);
    fetchData(""); // no date => latest
  }, []);

  // refetch whenever date changes (no buttons)
  useEffect(() => {
    if (!fetchedOnce.current) return;
    if (!reportDate) return;
    fetchData(reportDate);
  }, [reportDate]);

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            <div className="flex items-start justify-between">
              <div className="text-lg font-bold pb-3">DQ Reports</div>
              <div className="w-[240px]">
                <InputFieldset
                  id="report_date"
                  label="Report Date"
                  fieldName="report_date"
                  tooltipMsg="Report Date"
                  type="date"
                />
              </div>
            </div>

            <div className="mt-1 border rounded-md">
              <div
                className="flex items-center gap-2 px-3 py-2 cursor-pointer"
                onClick={() => setOpen((v) => !v)}
              >
                <i className={`ph ${open ? "ph-caret-down" : "ph-caret-right"}`} />
                <span className="font-semibold">Combined DQ Reports</span>
                <span className="text-xs text-gray-600">({normRows.length})</span>
              </div>

              {open && (
                <Box
                  className="ag-theme-alpine rounded-b-lg"
                  style={{ height: "600px", width: "100%" }}
                >
                  <AgGridReact
                    rowData={normRows}
                    columnDefs={columnDefs}
                    defaultColDef={{
                      flex: 1,
                      minWidth: 130,
                      sortable: true,
                      filter: true,
                      resizable: true,
                      enableValue: true,
                      enableRowGroup: true,
                      enablePivot: true,
                      enableCharts: true,
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
                    loading={loading}
                    animateRows={true}
                    enableRangeSelection={true}
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
