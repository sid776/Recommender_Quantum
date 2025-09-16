// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider } from "react-hook-form";
import { useMemo, useState } from "react";
import { InputFieldset, AppButton } from "../../elements";
import { Box, Collapsible, Skeleton } from "@chakra-ui/react";
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
      if (!(k in o)) o[k] = k === "report_date" ? "" : k === "risk_factor_id" ? "" : k === "rule_type" ? "" : k === "book" ? "" : 0;
    });
    YEARS.forEach((y) => {
      const v = o[y];
      o[y] = typeof v === "number" ? v : v == null || v === "" ? 0 : Number(v);
    });
    return o;
  });
}

function buildColumnDefs(rows) {
  const ordered = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  const seen = new Set(ordered);
  const extras = rows?.length
    ? Object.keys(rows[0]).filter((k) => !seen.has(k))
    : [];
  const keys = [...ordered, ...extras];

  return keys
    .filter((k) => !isAllNullOrEmpty(rows, k))
    .map((k) => {
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
  const methods = useForm();
  const { setValue, getValues } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [rootOpen, setRootOpen] = useState(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

  async function onRun() {
    setErrorText("");
    setRootOpen(false);
    const d = getValues("report_date");
    if (!d) {
      setRows([]);
      return;
    }
    setLoading(true);
    try {
      const res = await fetch(
        `${API_ENDPOINT}?report_date=${encodeURIComponent(d)}&limit=500`,
        { headers: { Accept: "application/json" } }
      );
      if (!res.ok) {
        setRows([]);
        setErrorText(`Server error ${res.status}`);
        return;
      }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);
      setRootOpen(false);
    } catch (e) {
      setRows([]);
      setErrorText(String(e || "Unknown error"));
    } finally {
      setLoading(false);
    }
  }

  function onReset() {
    setValue("report_date", "");
    setRows([]);
    setErrorText("");
    setRootOpen(false);
  }

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

            <div className="flex justify-end gap-4">
              <AppButton name="action" value="RESET" variant="secondary" onClick={onReset}>
                Reset
              </AppButton>
              <AppButton name="action" value="RUN" onClick={onRun}>
                Run
              </AppButton>
            </div>

            {errorText && (
              <div className="mt-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-2">
                {errorText}
              </div>
            )}

            {hasData && (
              <div className="mt-4 border rounded-md bg-gray-50">
                <div
                  className="flex items-center justify-between px-3 py-2 cursor-pointer"
                  onClick={() => setRootOpen((v) => !v)}
                >
                  <div className="flex items-center gap-2">
                    <i className={`ph ${rootOpen ? "ph-caret-down" : "ph-caret-right"}`} />
                    <span className="font-semibold">Combined DQ Reports</span>
                    <span className="text-xs text-gray-600">{normRows.length}</span>
                  </div>
                </div>

                <Collapsible.Root open={rootOpen} unmountOnExit>
                  <Collapsible.Content>
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
                          enableCharts: true,
                        }}
                        autoGroupColumnDef={{ headerName: "Group", minWidth: 260, pinned: "left" }}
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
                  </Collapsible.Content>
                </Collapsible.Root>
              </div>
            )}
          </div>
        </form>
      </FormProvider>

      <Box className="pt-3">
        {loading ? <Skeleton height="520px" rounded="md" /> : null}
      </Box>
    </Box>
  );
}
