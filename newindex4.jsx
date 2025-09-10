// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Box, Wrap, WrapItem, Button, Collapse, Flex, Text, Spacer, Skeleton } from "@chakra-ui/react";
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
  const [paramsOpen, setParamsOpen] = useState(true);

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
      const res = await fetch(url);
      if (!res.ok) {
        let detail = "";
        try {
          const body = await res.json();
          detail = body?.detail ? ` - ${typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail)}` : "";
        } catch {
          try {
            detail = ` - ${await res.text()}`;
          } catch {}
        }
        setErrorText(`Error: HTTP ${res.status}${detail}`);
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
      setErrorText("Error loading data");
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
      <Box className="overflow-auto" height="calc(100vh - 70px)" p={4}>
        <Box bg="white" rounded="lg" shadow="md">
          <Flex align="center" px={4} py={3} borderBottom="1px solid" borderColor="gray.100" cursor="pointer" onClick={() => setParamsOpen((o) => !o)}>
            <Text fontWeight="bold">Run Parameters</Text>
            <Spacer />
            <Button size="sm" onClick={(e) => { e.stopPropagation(); loadData(); }} isLoading={loading} colorScheme="green">Load</Button>
          </Flex>
          <Collapse in={paramsOpen} animateOpacity>
            <Box px={4} py={4}>
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
                {errorText ? (
                  <WrapItem>
                    <Box fontSize="sm" color="red.500" maxW="480px">{errorText}</Box>
                  </WrapItem>
                ) : null}
              </Wrap>
            </Box>
          </Collapse>
        </Box>

        <Box mt={4}>
          {loading && rows.length === 0 ? (
            <Skeleton height="240px" rounded="md" />
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
