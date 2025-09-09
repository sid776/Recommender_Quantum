// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useMemo, useState } from "react";
import { Box, Button, Flex, Select, Input } from "@chakra-ui/react";

// Try every plausible way ag-grid-react might export the component.
// This avoids the "got: object" crash if your bundler/versions change export shape.
import * as AgReact from "ag-grid-react";
import "ag-grid-enterprise";

// If your project doesn't already include AG Grid CSS globally, keep these:
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-alpine.css";

// Pick the right export at runtime
const GridComponent =
  // named export (usual)
  (AgReact && AgReact.AgGridReact) ||
  // default export (some CJS builds)
  (AgReact && AgReact.default && AgReact.default.AgGridReact) ||
  // default directly a component (rare)
  (AgReact && AgReact.default) ||
  // fallback (will be falsy if nothing matched)
  null;

// Quick guard to surface what we actually got
if (process.env.NODE_ENV !== "production") {
  // eslint-disable-next-line no-console
  console.log("ag-grid-react keys:", Object.keys(AgReact || {}));
  // eslint-disable-next-line no-console
  console.log("GridComponent typeof:", typeof GridComponent);
}

// DQ endpoints
const REPORTS = [
  { label: "DQ Summary", value: "summary" },
  { label: "DQ Staleness", value: "staleness" },
  { label: "DQ Outliers", value: "outliers" },
  { label: "DQ Availability", value: "availability" },
  { label: "DQ Reasonability", value: "reasonability" },
  { label: "DQ Schema", value: "schema" },
];

const REPORT_ENDPOINT = "/api/dq";

export default function CosmosReports() {
  const [reportName, setReportName] = useState(REPORTS[0].value);
  const [reportDate, setReportDate] = useState(
    () => new Date().toISOString().slice(0, 10)
  );
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isFilterVisible, setIsFilterVisible] = useState(false);

  const loadData = async () => {
    setLoading(true);
    try {
      const url = `${REPORT_ENDPOINT}/${reportName}?report_date=${reportDate}&limit=100`;
      const res = await fetch(url);
      const json = await res.json();
      setRows(Array.isArray(json) ? json : []);
    } catch (e) {
      console.error("CosmosReports load error:", e);
      setRows([]);
    } finally {
      setLoading(false);
    }
  };

  const columnDefs = useMemo(() => {
    if (!rows?.length) return [];
    return Object.keys(rows[0]).map((k) => ({
      headerName: k.replace(/_/g, " ").toUpperCase(),
      field: k,
      sortable: true,
      filter: true,
      resizable: true,
    }));
  }, [rows]);

  const defaultColDef = useMemo(
    () => ({ floatingFilter: isFilterVisible }),
    [isFilterVisible]
  );

  return (
    <Box p={4}>
      <Flex gap={3} align="center" mb={4} wrap="wrap">
        <Select
          value={reportName}
          onChange={(e) => setReportName(e.target.value)}
          width="280px"
        >
          {REPORTS.map((r) => (
            <option key={r.value} value={r.value}>
              {r.label}
            </option>
          ))}
        </Select>

        <Input
          type="date"
          value={reportDate}
          onChange={(e) => setReportDate(e.target.value)}
          width="180px"
        />

        <Button onClick={loadData} isLoading={loading}>
          Load
        </Button>

        <Button variant="outline" onClick={() => setIsFilterVisible((v) => !v)}>
          {isFilterVisible ? "Hide Filters" : "Show Filters"}
        </Button>
      </Flex>

      {/* If grid export couldn't be resolved, show a friendly error instead of crashing */}
      {!GridComponent ? (
        <Box
          p={4}
          mt={2}
          border="1px solid #e2e8f0"
          borderRadius="12px"
          bg="white"
        >
          <b>AG Grid React component was not found.</b>
          <div style={{ marginTop: 8 }}>
            Check your dependency and export shape:
            <pre style={{ whiteSpace: "pre-wrap", marginTop: 8 }}>
              {`import * as AgReact from "ag-grid-react";
console.log(Object.keys(AgReact)) // should include "AgGridReact"`}
            </pre>
            Make sure only one version of <code>ag-grid-react</code> is installed
            and there is no local file shadowing the module name.
          </div>
        </Box>
      ) : (
        <Box
          className="ag-theme-alpine"
          style={{ height: "600px", width: "100%", backgroundColor: "white" }}
        >
          <GridComponent
            rowData={rows}
            columnDefs={columnDefs}
            defaultColDef={defaultColDef}
            sideBar={{}}
            rowSelection="multiple"
            pagination={rows?.length > 5}
            paginationPageSize={50}
            paginationPageSizeSelector={[10, 20, 50, 100]}
          />
        </Box>
      )}
    </Box>
  );
}
