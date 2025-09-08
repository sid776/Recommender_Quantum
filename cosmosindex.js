import React, { useEffect, useMemo, useState } from "react";
import { Box, Button, Flex, Select, Input } from "@chakra-ui/react";
import ApplicableAgGrid from "../../components/ApplicableAgGrid"; // the one in your pic

const REPORT_ENDPOINT = "/api/cosmos"; // adjust to your API base path

export default function CosmosReports() {
  const [reportList, setReportList] = useState([]);
  const [reportName, setReportName] = useState("");
  const [reportDate, setReportDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  // load available reports for the dropdown
  useEffect(() => {
    fetch(`${REPORT_ENDPOINT}/reports`)
      .then((r) => r.json())
      .then((data) => {
        setReportList(data || []);
        if (data?.length) setReportName(data[0]);
      })
      .catch(() => setReportList([]));
  }, []);

  const loadData = async () => {
    if (!reportName || !reportDate) return;
    setLoading(true);
    try {
      const url = `${REPORT_ENDPOINT}/report/${encodeURIComponent(reportName)}?report_date=${reportDate}`;
      const res = await fetch(url);
      const json = await res.json();
      setRows(Array.isArray(json) ? json : []);
    } catch (e) {
      setRows([]);
    } finally {
      setLoading(false);
    }
  };

  // quick dynamic columns from first row (so it “just works” for all 6)
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

  return (
    <Box p={4}>
      <Flex gap={3} align="center" mb={4} wrap="wrap">
        <Select
          value={reportName}
          onChange={(e) => setReportName(e.target.value)}
          width="280px"
        >
          {reportList.map((r) => (
            <option key={r} value={r}>
              {r}
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

      <ApplicableAgGrid
        options={{
          title: reportName || "Cosmos Report",
          rowData: rows,
          COLUMN_DEFINITIONS: columnDefs,
          paginationPageSize: 50,
        }}
      />
    </Box>
  );
}
