// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useMemo, useState } from "react";
import { Box, Wrap, WrapItem } from "@chakra-ui/react";
import { useForm, FormProvider } from "react-hook-form";

import DropdownFieldSet from "../../elements/DropdownFieldSet.jsx";
import DynamicSelect from "../../elements/DynamicSelect.jsx";
import InputFieldSet from "../../elements/InputFieldSet.jsx";
import AppButton from "../../elements/AppButton";
import AgGridTable from "../../elements/AgGridTable.jsx";

/* ----------------------------- helpers ---------------------------------- */

// make labels nice
const prettify = (k) =>
  k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

// hide columns that are empty for all rows
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => r == null || r[k] == null || r[k] === "");

// preferred columns per report (optional: expand as you learn shapes)
const PREFERRED_BY_REPORT = {
  "DQ Summary": ["rule_type", "risk_factor_id", "book", "report_date", "2025", "2024"],
  Staleness: ["rule_type", "risk_factor_id", "report_date", "days_stale", "2025", "2024"],
  Outliers: ["risk_factor_id", "book", "report_date", "z_score"],
  Availability: ["table_name", "partition", "availability", "report_date"],
  Reasonability: ["risk_factor_id", "model", "report_date", "delta"],
  Schema: ["table", "column", "datatype", "nullable", "report_date"],
};

// YYYY-MM-DD → YYYYMMDD (if your backend prefers compact form)
const toYYYYMMDD = (d) => {
  if (!d) return "";
  const date = typeof d === "string" ? new Date(d) : d;
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${dd}`;
};

// builds column defs for any schema
const buildColumnDefs = (rows, reportName) => {
  if (!rows?.length) return [];

  // union of keys across rows (rows can be heterogeneous)
  const keySet = new Set();
  for (const r of rows) Object.keys(r || {}).forEach((k) => keySet.add(k));
  const allKeys = Array.from(keySet);

  // keep columns that have at least one non-empty value
  const visibleKeys = allKeys.filter((k) => !isAllNullOrEmpty(rows, k));

  // order: preferred (if present) → others; year columns at end desc
  const preferred = (PREFERRED_BY_REPORT[reportName] || []).filter((k) =>
    visibleKeys.includes(k)
  );

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
        : p.value,
  }));
};

/* ------------------------------ constants -------------------------------- */

const REPORTS = [
  { label: "DQ Summary", value: "summary" },
  { label: "DQ Staleness", value: "staleness" },
  { label: "DQ Outliers", value: "outliers" },
  { label: "DQ Availability", value: "availability" },
  { label: "DQ Reasonability", value: "reasonability" },
  { label: "DQ Schema", value: "schema" },
];

const REPORT_ENDPOINT = "/api/dq";

/* ------------------------------ component -------------------------------- */

export default function CosmosReports() {
  const methods = useForm({
    defaultValues: {
      reportName: REPORTS[0].value,
      reportDate: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    },
  });

  const { watch, setValue } = methods;
  const reportName = watch("reportName");
  const reportDate = watch("reportDate");

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  async function loadData() {
    setLoading(true);
    try {
      // build URL; if your backend wants YYYY-MM-DD just pass reportDate
      const dateParam = toYYYYMMDD(reportDate); // or use reportDate directly
      const url = `${REPORT_ENDPOINT}/${encodeURIComponent(
        reportName || ""
      )}?report_date=${dateParam}&limit=500`;

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
    } catch (e) {
      console.error("Failed to load report", e);
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  // auto-build columns for whatever schema this report returns
  const columnDefs = useMemo(() => buildColumnDefs(rows, REPORTS.find(r => r.value === reportName)?.label || reportName), [rows, reportName]);

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
              dataLoader={async () => REPORTS}  // ensure prop is 'dataLoader' in DynamicSelect
              onSelectionChange={(opt) => setValue("reportName", opt?.value)}
              defaultValue={REPORTS[0]}
            />
          </WrapItem>

          <WrapItem>
            <InputFieldSet
              id="reportDate"
              label="Report Date"
              type="date"
              registerOptions={{ required: "required" }}
            />
          </WrapItem>

          <WrapItem>
            <AppButton onClick={loadData} isLoading={loading} label="Load" />
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
