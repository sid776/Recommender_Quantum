// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Box, Wrap, WrapItem, Button } from "@chakra-ui/react";
import { useForm, FormProvider } from "react-hook-form";
import DynamicSelect from "../../elements/DynamicSelect.jsx";
import InputFieldSet from "../../elements/InputFieldSet.jsx";
import AgGridTable from "../../elements/AgGridTable.jsx";

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) => rows.every((r) => r == null || r[k] == null || r[k] === "");

const PREFERRED_BY_REPORT = {
  "DQ Summary": ["rule_type", "risk_factor_id", "book", "report_date", "2025", "2024"],
  "DQ Staleness": ["rule_type", "risk_factor_id", "report_date", "days_stale", "2025", "2024"],
  "DQ Outliers": ["risk_factor_id", "book", "report_date", "z_score"],
  "DQ Availability": ["table_name", "partition", "availability", "report_date"],
  "DQ Reasonability": ["risk_factor_id", "model", "report_date", "delta"],
  "DQ Schema": ["table", "column", "datatype", "nullable", "report_date"]
};

const toYYYYMMDD = (d) => {
  if (!d) return "";
  const date = typeof d === "string" ? new Date(d) : d;
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${dd}`;
};

const buildColumnDefs = (rows, reportLabel) => {
  if (!rows?.length) return [];
  const keySet = new Set();
  for (const r of rows) Object.keys(r || {}).forEach((k) => keySet.add(k));
  const allKeys = Array.from(keySet);
  const visibleKeys = allKeys.filter((k) => !isAllNullOrEmpty(rows, k));
  const preferred = (PREFERRED_BY_REPORT[reportLabel] || []).filter((k) => visibleKeys.includes(k));
  const nonPreferred = visibleKeys
    .filter((k) => !preferred.includes(k))
    .sort((a, b) => {
      const ay = /^\d{4}$/.test(a), by = /^\d{4}$/.test(b);
      if (ay && by) return Number(b) - Number(a);
      if (ay) return 1;
      if (by) return -1;
      return a.localeCompare(b);
    });
  const ordered = [...preferred, ...nonPreferred];
  return ordered.map((k) => ({
    headerName: prettify(k),
    field: k,
    sortable: true,
    filter: true,
    resizable: true,
    minWidth: /^\d{4}$/.test(k) ? 90 : 140,
    valueFormatter: (p) =>
      k === "report_date" && typeof p.value === "string" && /^\d{8}$/.test(p.value)
        ? `${p.value.slice(0, 4)}-${p.value.slice(4, 6)}-${p.value.slice(6, 8)}`
        : p.value
  }));
};

const REPORTS = [
  { label: "DQ Summary", value: "summary" },
  { label: "DQ Staleness", value: "staleness" },
  { label: "DQ Outliers", value: "outliers" },
  { label: "DQ Availability", value: "availability" },
  { label: "DQ Reasonability", value: "reasonability" },
  { label: "DQ Schema", value: "schema" }
];

const REPORT_ENDPOINT = "/api/dq";

export default function CosmosReports() {
  const methods = useForm({
    defaultValues: {
      reportName: REPORTS[0].value,
      reportDate: new Date().toISOString().slice(0, 10)
    }
  });

  const { watch, setValue } = methods;
  const reportName = watch("reportName");
  const reportDate = watch("reportDate");

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  async function loadData() {
    setLoading(true);
    try {
      const dateParam = toYYYYMMDD(reportDate);
      const url = `${REPORT_ENDPOINT}/${encodeURIComponent(reportName || "")}?report_date=${dateParam}&limit=500`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      const data = Array.isArray(json)
        ? json
        : Array.isArray(json.data)
        ? json.data
        : Array.isArray(json.rows)
        ? json.rows
        : [];
      setRows(data);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (reportName && reportDate) loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reportName, reportDate]);

  const reportLabel = REPORTS.find((r) => r.value === reportName)?.label || reportName;
  const columnDefs = useMemo(() => buildColumnDefs(rows, reportLabel), [rows, reportLabel]);

  return (
    <FormProvider {...methods}>
      <Box p={4}>
        <Wrap align="center" mb={4} spacing="16px">
          <WrapItem>
            <DynamicSelect
              id="reportName"
              fieldName="reportName"
              label="Report"
              placeholder="Select report"
              dataLoader={async () => REPORTS}
              onSelectionChange={(opt) => setValue("reportName", opt?.value)}
              defaultValue={REPORTS[0]}
            />
          </WrapItem>

          <WrapItem>
            <InputFieldSet
              id="reportDate"
              fieldName="reportDate"
              label="Report Date"
              type="date"
              registerOptions={{ required: "required" }}
            />
          </WrapItem>

          <WrapItem>
            <Button onClick={loadData} isLoading={loading} colorScheme="green">
              Load
            </Button>
          </WrapItem>
        </Wrap>

        <AgGridTable
          rowData={rows}
          columnDefs={columnDefs}
          pagination={true}
          paginationPageSize={50}
          defaultColDef={{ sortable: true, filter: true, resizable: true }}
        />
      </Box>
    </FormProvider>
  );
}
