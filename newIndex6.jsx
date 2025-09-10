// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useMemo, useState } from "react";
import { Box, Wrap, WrapItem, Button, Skeleton, HStack, Text, Collapsible } from "@chakra-ui/react";
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
      reportName: REPORTS[0],
      reportDate: new Date().toISOString().slice(0, 10)
    }
  });

  const { setValue, getValues, watch } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [lastUrl, setLastUrl] = useState("");
  const [panelOpen, setPanelOpen] = useState(true);
  const [menuSpace, setMenuSpace] = useState(false); // pushes grid down while select is open/focused

  async function loadData() {
    setLoading(true);
    try {
      const name = getValues("reportName");
      const nameVal = typeof name === "string" ? name : name?.value;
      const dateStr = getValues("reportDate");
      if (!nameVal || !dateStr) {
        setRows([]);
        setLastUrl("");
        return;
      }
      const url = `${REPORT_ENDPOINT}/${encodeURIComponent(nameVal)}?report_date=${encodeURIComponent(dateStr)}&limit=500`;
      setLastUrl(url);

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

  const reportObj = watch("reportName");
  const reportLabel =
    (typeof reportObj === "object" ? reportObj?.label : REPORTS.find((r) => r.value === reportObj)?.label) ||
    String(reportObj || "");

  const columnDefs = useMemo(() => buildColumnDefs(rows, reportLabel), [rows, reportLabel]);

  return (
    <FormProvider {...methods}>
      <Box className="mx-auto max-w-[1400px] space-y-4 p-4" style={{ overflow: "visible" }}>
        <Box
          className="bg-white rounded-lg shadow-lg"
          style={{ position: "relative", zIndex: 9999, overflow: "visible" }}
        >
          <Collapsible.Root open={panelOpen} onOpenChange={setPanelOpen}>
            <Box
              className="flex items-center justify-between px-4 py-3 cursor-pointer select-none"
              onClick={() => setPanelOpen((v) => !v)}
            >
              <Text fontSize="lg" fontWeight="bold">{reportLabel}</Text>
              <HStack spacing={3}>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={(e) => { e.stopPropagation(); setRows([]); setLastUrl(""); }}
                >
                  Clear
                </Button>
                <Button
                  size="sm"
                  colorScheme="green"
                  onClick={(e) => { e.stopPropagation(); loadData(); }}
                  isLoading={loading}
                >
                  Load
                </Button>
                <span className={`transition-transform ${panelOpen ? "rotate-180" : "rotate-0"}`}>▾</span>
              </HStack>
            </Box>

            <Collapsible.Content>
              <Box
                className="px-4 pb-4"
                style={{ overflow: "visible" }}
                onFocusCapture={() => setMenuSpace(true)}
                onBlurCapture={() => setMenuSpace(false)}
              >
                <Wrap align="center" spacing="16px" style={{ overflow: "visible" }}>
                  <WrapItem style={{ minWidth: 280, position: "relative", zIndex: 10000, overflow: "visible" }}>
                    <DynamicSelect
                      id="reportName"
                      fieldName="reportName"
                      label="Report"
                      placeholder="Select report"
                      dataLoader={async () => REPORTS}
                      onSelectionChange={(opt) => setValue("reportName", opt)}
                      defaultValue={REPORTS[0]}
                    />
                  </WrapItem>

                  <WrapItem style={{ minWidth: 220 }}>
                    <InputFieldSet
                      id="reportDate"
                      fieldName="reportDate"
                      label="Report Date (optional)"
                      type="date"
                    />
                  </WrapItem>
                </Wrap>

                {lastUrl ? (
                  <Box mt={2} fontSize="xs" color="gray.600" wordBreak="break-all">
                    {lastUrl}
                  </Box>
                ) : null}

                {menuSpace ? <Box height="320px" /> : null}
              </Box>
            </Collapsible.Content>
          </Collapsible.Root>
        </Box>

        <Box
          className="bg-white rounded-lg shadow-lg p-2"
          style={{ height: "calc(100vh - 260px)", position: "relative", zIndex: 1, overflow: "visible" }}
        >
          {loading ? (
            <Skeleton height="100%" rounded="md" />
          ) : (
            <AgGridTable
              rowData={rows}
              columnDefs={columnDefs}
              pagination={true}
              paginationPageSize={50}
              defaultColDef={{ sortable: true, filter: true, resizable: true }}
              style={{ height: "100%" }}
            />
          )}
        </Box>
      </Box>
    </FormProvider>
  );
}
