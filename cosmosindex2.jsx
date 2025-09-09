import React, { useEffect, useState, useMemo } from "react";
import { Box, Button, Flex, Select, Input } from "@chakra-ui/react";
import ApplicableAgGrid from "../ModelConfiguration/ApplicableAgGrid";

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
    new Date().toISOString().slice(0, 10)
  );
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const loadData = async () => {
    setLoading(true);
    try {
      const url = `${REPORT_ENDPOINT}/${reportName}?report_date=${reportDate}`;
      const res = await fetch(url);
      const json = await res.json();
      setRows(Array.isArray(json) ? json : []);
    } catch (e) {
      setRows([]);
    } finally {
      setLoading(false);
    }
  };

  const columnDefs = useMemo(() => {
    if (!rows.length) return [];
    return Object.keys(rows[0]).map((k) => ({
      headerName: k.replace(/_/g, " ").toUpperCase(),
      field: k,
      sortable: true,
      filter: true,
      resizable: true,
    }));
  }, [rows]);

  const gridOptions = {
    title: reportName,
    rowData: rows,
    COLUMN_DEFINITIONS: columnDefs,
    setSelectedRows: () => {},
    gridRef: null,
    autoGroupColumnDef: undefined,
    animateRows: true,
    suppressRowClickSelection: true,
    groupSelectsChildren: false,
    paginationPageSize: 50,
  };

  return (
    <Box p={4}>
      <Flex gap={3} mb={4} align="center">
        <Select
          value={reportName}
          onChange={(e) => setReportName(e.target.value)}
          width="220px"
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
      </Flex>

      <ApplicableAgGrid options={gridOptions} />
    </Box>
  );
}
