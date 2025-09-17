// frontend/src/components/pages/VAReports/index.jsx
import { useEffect, useMemo, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { Box, Skeleton } from "@chakra-ui/react";
import { InputFieldset } from "../../elements";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API = {
  latest: "/api/va/latest_cob",
  bookCounts: "/api/va/book_counts",
  shockCounts: "/api/va/risk_shocks_counts",
  sensiBookCounts: "/api/va/sensitivities_book_counts",
  runCounts: "/api/va/valuation_run_counts",
};

async function getJSON(url) {
  const r = await fetch(url, { headers: { Accept: "application/json" } });
  if (!r.ok) throw new Error(String(r.status));
  return r.json();
}

const headerClass =
  "bg-white font-semibold text-gray-700 border-b border-gray-200";

function gridBox(children) {
  return (
    <Box
      className="ag-theme-alpine rounded-lg shadow-md"
      style={{ height: "420px", width: "100%" }}
    >
      {children}
    </Box>
  );
}

export default function VAReports() {
  const methods = useForm();
  const { setValue, watch } = methods;

  const [loading, setLoading] = useState(true);
  const [bookRows, setBookRows] = useState([]);
  const [shockRows, setShockRows] = useState([]);
  const [sensiRows, setSensiRows] = useState([]);
  const [runRows, setRunRows] = useState([]);

  const cobDate = watch("cob_date");

  // load default date then data
  useEffect(() => {
    (async () => {
      try {
        const j = await getJSON(API.latest);
        const d = j?.cob_date || "";
        setValue("cob_date", d);
      } catch {
        setValue("cob_date", "");
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // whenever date changes, refresh all sections
  useEffect(() => {
    if (!cobDate) return;
    setLoading(true);
    (async () => {
      try {
        const qs = `?cob_date=${encodeURIComponent(cobDate)}&limit=1000`;
        const [books, shocks, sensis, runs] = await Promise.all([
          getJSON(API.bookCounts + qs),
          getJSON(API.shockCounts + qs),
          getJSON(API.sensiBookCounts + qs),
          getJSON(API.runCounts + `?cob_date=${encodeURIComponent(cobDate)}`),
        ]);
        setBookRows(Array.isArray(books) ? books : []);
        setShockRows(Array.isArray(shocks) ? shocks : []);
        setSensiRows(Array.isArray(sensis) ? sensis : []);
        setRunRows(Array.isArray(runs) ? runs : []);
      } catch {
        setBookRows([]); setShockRows([]); setSensiRows([]); setRunRows([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [cobDate]);

  const bookCols = useMemo(
    () => [
      { headerName: "Book", field: "book", minWidth: 200, filter: "agTextColumnFilter" },
      { headerName: "Count", field: "count", type: "numericColumn", minWidth: 120 },
    ],
    []
  );

  const shockCols = useMemo(
    () => [
      { headerName: "Risk Factor Id", field: "risk_factor_id", minWidth: 240, filter: "agTextColumnFilter" },
      { headerName: "Curve", field: "curve", minWidth: 140, filter: "agTextColumnFilter" },
      { headerName: "Count", field: "count", type: "numericColumn", minWidth: 120 },
    ],
    []
  );

  const sensiCols = useMemo(
    () => [
      { headerName: "Book", field: "book", minWidth: 220, filter: "agTextColumnFilter" },
      { headerName: "Count", field: "count", type: "numericColumn", minWidth: 120 },
    ],
    []
  );

  const runCols = useMemo(
    () => [
      { headerName: "Run Count", field: "run_count", type: "numericColumn", minWidth: 140 },
    ],
    []
  );

  const defaultColDef = useMemo(
    () => ({
      sortable: true,
      resizable: true,
      filter: true,
      headerClass,
      flex: 1,
      minWidth: 120,
    }),
    []
  );

  const sideBar = useMemo(
    () => ({
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
    }),
    []
  );

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            <div className="text-lg font-bold pb-1">VA Reports</div>

            <div className="flex items-center justify-end">
              <InputFieldset
                id="cob_date"
                label="COB"
                fieldName="cob_date"
                tooltipMsg="COB"
                type="date"
                style={{ width: "240px" }}
              />
            </div>
          </div>

          <div className="p-4 bg-white shadow-md rounded-lg">
            <div className="text-base font-semibold pb-2">Book Counts</div>
            {loading ? (
              <Skeleton height="420px" rounded="md" />
            ) : (
              gridBox(
                <AgGridReact
                  rowData={bookRows}
                  columnDefs={bookCols}
                  defaultColDef={defaultColDef}
                  sideBar={sideBar}
                  animateRows
                />
              )
            )}
          </div>

          <div className="p-4 bg-white shadow-md rounded-lg">
            <div className="text-base font-semibold pb-2">Risk Shocks Counts</div>
            {loading ? (
              <Skeleton height="420px" rounded="md" />
            ) : (
              gridBox(
                <AgGridReact
                  rowData={shockRows}
                  columnDefs={shockCols}
                  defaultColDef={defaultColDef}
                  sideBar={sideBar}
                  animateRows
                />
              )
            )}
          </div>

          <div className="p-4 bg-white shadow-md rounded-lg">
            <div className="text-base font-semibold pb-2">Sensitivity Book Counts</div>
            {loading ? (
              <Skeleton height="420px" rounded="md" />
            ) : (
              gridBox(
                <AgGridReact
                  rowData={sensiRows}
                  columnDefs={sensiCols}
                  defaultColDef={defaultColDef}
                  sideBar={sideBar}
                  animateRows
                />
              )
            )}
          </div>

          <div className="p-4 bg-white shadow-md rounded-lg">
            <div className="text-base font-semibold pb-2">Valuation Run Counts</div>
            {loading ? (
              <Skeleton height="200px" rounded="md" />
            ) : (
              gridBox(
                <AgGridReact
                  rowData={runRows}
                  columnDefs={runCols}
                  defaultColDef={defaultColDef}
                  sideBar={sideBar}
                  animateRows
                />
              )
            )}
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
