// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider } from "react-hook-form";
import { useMemo, useState } from "react";
import { InputFieldset, AppButton } from "../../elements";
import { Box, Collapsible, Skeleton } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => r == null || r[k] == null || r[k] === "");

function buildColumnDefs(rows) {
  if (!rows?.length) return [];
  const keys = new Set();
  rows.forEach((r) => Object.keys(r || {}).forEach((k) => keys.add(k)));
  const visible = Array.from(keys).filter((k) => !isAllNullOrEmpty(rows, k));
  return visible.map((k) => ({
    headerName: prettify(k),
    field: k,
    sortable: true,
    filter: true,
    resizable: true,
    minWidth: /^\d{4}$/.test(k) ? 90 : 140,
  }));
}

export default function CosmosReports() {
  const methods = useForm();
  const { setValue, getValues } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [rootOpen, setRootOpen] = useState(false); // controls the single parent row

  const columnDefs = useMemo(() => buildColumnDefs(rows), [rows]);
  const hasData = rows.length > 0;

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

      const data = await res.json().catch(() => []);
      const out = Array.isArray(data) ? data : Array.isArray(data?.rows) ? data.rows : [];
      setRows(out || []);
      setRootOpen(false); // start collapsed
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
                    <span className="text-xs text-gray-600">{rows.length}</span>
                  </div>
                </div>

                <Collapsible.Root open={rootOpen} unmountOnExit>
                  <Collapsible.Content>
                    <Box
                      className="ag-theme-alpine rounded-lg shadow-md"
                      style={{ height: "600px", width: "100%" }}
                    >
                      <AgGridReact
                        rowData={rows}
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
                        autoGroupColumnDef={{ minWidth: 200, pinned: "left" }}
                        // Tool panel buttons only (closed by default). Adds "Columns" AND "Filter" buttons.
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
                        enableRangeSelection={true}
                        animateRows={true}
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
