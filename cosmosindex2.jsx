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
##################################
import React, { useMemo, useState } from "react";
import { Box, Wrap, WrapItem } from "@chakra-ui/react";
import DropdownFieldSet from "../../elements/DropdownFieldSet";
import InputFieldSet from "../../elements/InputFieldSet";
import AppButton from "../../elements/AppButton";
import AgGridTable from "../../elements/AgGridTable";

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
  const [reportDate, setReportDate] = useState(new Date().toISOString().slice(0, 10));
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const loadData = async () => {
    setLoading(true);
    try {
      const url = `${REPORT_ENDPOINT}/${reportName}?report_date=${reportDate}&limit=100`;
      const res = await fetch(url);
      const json = await res.json();
      setRows(Array.isArray(json) ? json : []);
    } catch {
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

  return (
    <Box p={4}>
      <Wrap spacing={4} align="center">
        <WrapItem>
          <DropdownFieldSet
            id="dq-report"
            label="Report"
            options={REPORTS}
            value={reportName}
            onSelectionChange={(v) => setReportName(v?.value ?? v)}
            isMultiSelect={false}
            isSearchable={false}
            style={{ width: "260px" }}
          />
        </WrapItem>

        <WrapItem>
          <InputFieldSet
            id="report-date"
            label="Report Date"
            type="date"
            value={reportDate}
            onChange={(e) => setReportDate(e.target.value)}
            style={{ width: "200px" }}
          />
        </WrapItem>

        <WrapItem>
          <AppButton onClick={loadData} loading={loading}>
            Load
          </AppButton>
        </WrapItem>
      </Wrap>

      <Box mt={4} className="ag-theme-alpine" style={{ height: "520px", width: "100%" }}>
        <AgGridTable
          rowData={rows}
          columnDefs={columnDefs}
          rowSelection="multiple"
          pagination
          paginationPageSize={50}
        />
      </Box>
    </Box>
  );
}


