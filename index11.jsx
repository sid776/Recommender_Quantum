// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Box, HStack, Button, Skeleton, Text, Grid, GridItem } from "@chakra-ui/react";
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
      reportName: null,
      reportDate: ""
    }
  });

  const { setValue, getValues, watch } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [menuSpace, setMenuSpace] = useState(false);

  const reportObj = watch("reportName");

  useEffect(() => {
    if (!reportObj) {
      setRows([]);
      setValue("reportDate", "");
    }
  }, [reportObj, setValue]);

  async function loadData() {
    setLoading(true);
    try {
      const sel = getValues("reportName");
      const nameVal = typeof sel === "string" ? sel : sel?.value;
      const dateStr = getValues("reportDate");
      if (!nameVal || !dateStr) {
        setRows([]);
        return;
      }
      const url = `${REPORT_ENDPOINT}/${encodeURIComponent(nameVal)}?report_date=${encodeURIComponent(dateStr)}&limit=500`;
      const res = await fetch(url);
      if (!res.ok) {
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
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  const reportLabel =
    (typeof reportObj === "object" ? reportObj?.label : REPORTS.find((r) => r.value === reportObj)?.label) || "";

  const columnDefs = useMemo(() => buildColumnDefs(rows, reportLabel), [rows, reportLabel]);
  const hasData = rows && rows.length > 0;

  return (
    <FormProvider {...methods}>
      {/* match calculator card vertical alignment with a tiny negative top margin */}
      <Box className="mx-auto max-w-[1400px] p-4" style={{ overflow: "visible", marginTop: "-6px" }}>
        <Box className="bg-white rounded-lg shadow-lg p-4" style={{ position: "relative", zIndex: 1, overflow: "visible" }}>
          <HStack justify="space-between" align="center" mb={3}>
            <Text fontSize="lg" fontWeight="bold">DQ Reports</Text>
            <HStack spacing={3}>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  setRows([]);
                  setValue("reportName", null);
                  setValue("reportDate", "");
                }}
              >
                Clear
              </Button>
              <Button size="sm" colorScheme="green" onClick={loadData} isLoading={loading}>
                Submit
              </Button>
            </HStack>
          </HStack>

          <Grid
            templateColumns="1fr 1fr"
            gap="6px 24px"
            alignItems="end"
            style={{ overflow: "visible", width: "100%" }}
            onFocusCapture={() => setMenuSpace(true)}
            onBlurCapture={() => setMenuSpace(false)}
          >
            <GridItem>
              <Text fontSize="sm" fontWeight="bold" color="gray.700">Reports</Text>
            </GridItem>
            <GridItem>
              <Text fontSize="sm" fontWeight="bold" color="gray.700">Report Date</Text>
            </GridItem>

            <GridItem minW="320px" maxW="320px" position="relative" zIndex={10000} style={{ display: "flex", alignItems: "center" }}>
              <DynamicSelect
                id="reportName"
                fieldName="reportName"
                label=""
                placeholder="Select report"
                dataLoader={async () => REPORTS}
                onSelectionChange={(opt) => {
                  if (!opt) {
                    setValue("reportName", null);
                    setValue("reportDate", "");
                    setRows([]);
                    return;
                  }
                  setValue("reportName", opt);
                }}
                defaultValue={null}
              />
            </GridItem>

            {/* date input width matched to calculator (~260px) and aligned baseline */}
            <GridItem minW="260px" maxW="260px" w="260px" style={{ display: "flex", alignItems: "center" }}>
              <InputFieldSet id="reportDate" fieldName="reportDate" label="" type="date" />
            </GridItem>
          </Grid>

          {/* spacer only while select is interacting so dropdown never hides behind grid */}
          <Box style={{ height: menuSpace ? "280px" : 0, transition: "height 120ms" }} />

          {loading ? (
            <Box mt={2}><Skeleton height="520px" rounded="md" /></Box>
          ) : hasData ? (
            <Box mt={2} style={{ height: "calc(100vh - 320px)" }}>
              <AgGridTable
                rowData={rows}
                columnDefs={columnDefs}
                pagination={true}
                paginationPageSize={50}
                defaultColDef={{ sortable: true, filter: true, resizable: true }}
                style={{ height: "100%" }}
              />
            </Box>
          ) : null}
        </Box>
      </Box>
    </FormProvider>
  );
}
