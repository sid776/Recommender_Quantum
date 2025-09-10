// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Box, Wrap, WrapItem, Button, Skeleton, Alert, AlertIcon, HStack, Text } from "@chakra-ui/react";
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

const SAMPLE_ROWS = [
  {
    rule_type: "staleness_check",
    risk_factor_id: "FX_CNCNH_USD",
    book: "BK001",
    report_date: "20250404",
    "2024": 0,
    "2025": 1
  },
  {
    rule_type: "staleness_check",
    risk_factor_id: "IR_USD_LIBOR_3M",
    book: "BK002",
    report_date: "20250404",
    "2024": 0,
    "2025": 0
  }
];

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
  const [errorText, setErrorText] = useState("");
  const [lastUrl, setLastUrl] = useState("");

  async function loadData() {
    setLoading(true);
    setErrorText("");
    try {
      const name = methods.getValues("reportName");
      const dateStr = methods.getValues("reportDate");
      if (!name || !dateStr) {
        setRows([]);
        return;
      }
      const url = `${REPORT_ENDPOINT}/${encodeURIComponent(name)}?report_date=${encodeURIComponent(dateStr)}&limit=500`;
      setLastUrl(url);
      const res = await fetch(url);
      if (!res.ok) {
        let detail = "";
        try {
          const body = await res.json();
          detail = body?.detail ? ` - ${JSON.stringify(body.detail)}` : "";
        } catch {
          try {
            detail = ` - ${await res.text()}`;
          } catch {}
        }
        setErrorText(`HTTP ${res.status}${detail}`);
        setRows([]);
        return;
      }
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
      setErrorText("Network or server error");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (reportName && reportDate) loadData();
  }, [reportName, reportDate]);

  const reportLabel = REPORTS.find((r) => r.value === reportName)?.label || reportName;
  const columnDefs = useMemo(() => buildColumnDefs(rows, reportLabel), [rows, reportLabel]);

  return (
    <FormProvider {...methods}>
      <Box className="mx-auto max-w-[1400px] space-y-4 p-4">
        <Box className="bg-white rounded-lg shadow-lg p-4">
          <HStack justify="space-between" align="center" mb={2}>
            <Text fontSize="lg" fontWeight="bold">{reportLabel}</Text>
            <HStack spacing={3}>
              <Button size="sm" colorScheme="gray" variant="outline" onClick={() => { setRows(SAMPLE_ROWS); setErrorText(""); }}>
                Use Sample Data
              </Button>
              <Button size="sm" colorScheme="green" onClick={loadData} isLoading={loading}>
                Load
              </Button>
            </HStack>
          </HStack>

          <Wrap align="center" spacing="16px">
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
          </Wrap>

          {errorText ? (
            <Alert status="error" mt={3}>
              <AlertIcon />
              {errorText}
            </Alert>
          ) : null}

          {lastUrl ? (
            <Box mt={2} fontSize="xs" color="gray.600" wordBreak="break-all">
              {lastUrl}
            </Box>
          ) : null}
        </Box>

        <Box className="bg-white rounded-lg shadow-lg p-2">
          {loading ? (
            <Skeleton height="520px" rounded="md" />
          ) : (
            <AgGridTable
              rowData={rows}
              columnDefs={columnDefs}
              pagination={true}
              paginationPageSize={50}
              defaultColDef={{ sortable: true, filter: true, resizable: true }}
            />
          )}
        </Box>
      </Box>
    </FormProvider>
  );
}
