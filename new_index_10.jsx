// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box, Skeleton, Collapsible } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const YEARS = ["2021", "2022", "2023", "2024", "2025"];

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => r == null || r[k] == null || r[k] === "");

function normalizeRows(rows) {
  const base = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  return (rows || []).map((r) => {
    const o = { ...r };
    base.forEach((k) => {
      if (!(k in o)) {
        o[k] =
          k === "report_date" ? "" :
          k === "risk_factor_id" ? "" :
          k === "rule_type" ? "" :
          k === "book" ? "" : 0;
      }
    });
    YEARS.forEach((y) => {
      const v = o[y];
      o[y] = typeof v === "number" ? v : v == null || v === "" ? 0 : Number(v);
    });
    const d = String(o.report_date || "");
    if (/^\d{8}$/.test(d)) o.report_date = `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`;
    return o;
  });
}

function buildColumnDefs(rows) {
  const ordered = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  const seen = new Set(ordered);
  const extras = rows?.length ? Object.keys(rows[0]).filter((k) => !seen.has(k)) : [];
  const keys = [...ordered, ...extras];

  return keys
    .filter((k) => !isAllNullOrEmpty(rows, k))
    .map((k) => {
      const col = {
        headerName: prettify(k),
        headerValueGetter: () => prettify(k),
        field: k,
        sortable: true,
        filter: true,
        resizable: true,
        minWidth: YEARS.includes(k) ? 90 : 140
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
  const { setValue, getValues, control } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false); // collapsed by default

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

  // Fetch latest date on mount and set it (preselected)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoading(true);
        const res = await fetch(`${API_ENDPOINT}?limit=1`, { headers: { Accept: "application/json" } });
        const json = await res.json().catch(() => []);
        const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
        if (!cancelled && data?.length) {
          const d = String(data[0]?.report_date || "");
          const iso = /^\d{8}$/.test(d) ? `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}` : d;
          if (iso) setValue("report_date", iso);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [setValue]);

  // Auto-load data whenever the user changes the date
  const reportDate = useWatch({ control, name: "report_date" });
  useEffect(() => {
    let cancelled = false;
    async function load() {
      const d = getValues("report_date");
      if (!d) {
        setRows([]);
        setOpen(false);
        return;
      }
      setLoading(true);
      try {
        const res = await fetch(
          `${API_ENDPOINT}?report_date=${encodeURIComponent(d)}&limit=500`,
          { headers: { Accept: "application/json" } }
        );
        const json = await res.json().catch(() => []);
        const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
        if (!cancelled) {
          setRows(data || []);
          setOpen(false); // keep closed until user expands
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [reportDate, getValues]);

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            <div className="flex gap-4 pb-3">
              <span className="text-lg font-bold">DQ Reports</span>
            </div>

            <div className="flex gap-4 pb-3">
              <InputFieldset
                id="report_date"
                label="Report Date"
                fieldName="report_date"
                tooltipMsg="Report Date"
                type="date"
              />
            </div>

            <div className="mt-2 border rounded-md bg-gray-50">
              <div
                className="flex items-center justify-between px-3 py-2 cursor-pointer"
                onClick={() => setOpen((v) => !v)}
              >
                <div className="flex items-center gap-2">
                  <i className={`ph ${open ? "ph-caret-down" : "ph-caret-right"}`} />
                  <span className="font-semibold">Combined DQ Reports</span>
                  {hasData ? (
                    <span className="text-xs text-gray-600">( {normRows.length} )</span>
                  ) : (
                    <span className="text-xs text-gray-600">( 0 )</span>
                  )}
                </div>
              </div>

              <Collapsible.Root open={open} unmountOnExit>
                <Collapsible.Content>
                  {loading ? (
                    <Skeleton height="520px" rounded="md" />
                  ) : hasData ? (
                    <Box
                      className="ag-theme-alpine rounded-lg shadow-md"
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
                          enableCharts: true
                        }}
                        autoGroupColumnDef={{
                          headerName: "Group",
                          minWidth: 260,
                          pinned: "left"
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
                              toolPanelParams: { suppressPivotMode: true }
                            },
                            {
                              id: "filters",
                              labelDefault: "Filter",
                              iconKey: "filter",
                              toolPanel: "agFiltersToolPanel"
                            }
                          ]
                        }}
                        pivotMode={false}
                        animateRows={true}
                        enableRangeSelection={true}
                        getContextMenuItems={(params) => {
                          const def = params.defaultItems;
                          const custom = ["chartRange", "pivotChart"];
                          return [...def, "separator", ...custom];
                        }}
                      />
                    </Box>
                  ) : null}
                </Collapsible.Content>
              </Collapsible.Root>
            </div>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
