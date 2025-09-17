// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider } from "react-hook-form";
import { useEffect, useMemo, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box, Skeleton } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const YEARS = ["2021", "2022", "2023", "2024", "2025"];

const prettify = (k) =>
  k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => r == null || r[k] == null || r[k] === "");

function normalizeRows(rows) {
  const base = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  return (rows || []).map((r) => {
    const o = { ...r };
    base.forEach((k) => {
      if (!(k in o))
        o[k] =
          k === "report_date" ||
          k === "risk_factor_id" ||
          k === "rule_type" ||
          k === "book"
            ? ""
            : 0;
    });
    YEARS.forEach((y) => {
      const v = o[y];
      o[y] =
        typeof v === "number" ? v : v == null || v === "" ? 0 : Number(v);
    });
    return o;
  });
}

function buildColumnDefs(rows) {
  const ordered = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  const seen = new Set(ordered);
  const extras = rows?.length
    ? Object.keys(rows[0]).filter((k) => !seen.has(k))
    : [];
  const keys = [...ordered, ...extras];

  return keys
    .filter((k) => !isAllNullOrEmpty(rows, k))
    .map((k) => {
      const col = {
        headerName: prettify(k),
        field: k,
        sortable: true,
        filter: true,
        resizable: true,
        minWidth: YEARS.includes(k) ? 90 : 140,
        headerClass: "ag-header-bold"
      };
      if (k === "rule_type" || k === "book") {
        col.rowGroup = true;
        col.hide = true;
      }
      if (YEARS.includes(k)) {
        col.type = "numericColumn";
        col.aggFunc = "sum";
        col.valueParser = (p) => Number(p.newValue ?? 0);
      }
      return col;
    });
}

export default function CosmosReports() {
  const methods = useForm();
  const { setValue, getValues } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [showFloatingFilters, setShowFloatingFilters] = useState(false);

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

  useEffect(() => {
    async function fetchLatest() {
      try {
        const res = await fetch(
          `${API_ENDPOINT}?limit=1&latest=true`,
          { headers: { Accept: "application/json" } }
        );
        const json = await res.json().catch(() => []);
        const data = Array.isArray(json) ? json : [];
        if (data.length) {
          const latestDate = data[0].report_date;
          setValue("report_date", latestDate);
          await onRun(latestDate);
        }
      } catch {
        /* ignore */
      }
    }
    fetchLatest();
  }, []);

  async function onRun(dateOverride) {
    setErrorText("");
    const d = dateOverride || getValues("report_date");
    if (!d) {
      setRows([]);
      return;
    }
    setLoading(true);
    try {
      const res = await fetch(
        `${API_ENDPOINT}?report_date=${encodeURIComponent(d)}&limit=500`,
        { headers: { Accept: "application/json" } }
      );
      if (!res.ok) {
        setRows([]);
        setErrorText(`Server error ${res.status}`);
        return;
      }
      const json = await res.json().catch(() => []);
      const data = Array.isArray(json) ? json : Array.isArray(json?.rows) ? json.rows : [];
      setRows(data || []);
    } catch (e) {
      setRows([]);
      setErrorText(String(e || "Unknown error"));
    } finally {
      setLoading(false);
    }
  }

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div
            className="p-4 bg-white shadow-md rounded-lg flex justify-between items-center"
            style={{ marginTop: -16 }}
          >
            <div className="flex gap-4 items-center">
              <span className="text-lg font-bold">DQ Reports</span>
              <InputFieldset
                id="report_date"
                label="Report Date"
                fieldName="report_date"
                tooltipMsg="Report Date"
                type="date"
                onChange={(e) => onRun(e.target.value)}
              />
            </div>
            <button
              type="button"
              title="Toggle Filters"
              className={`h-9 w-9 rounded-md border flex items-center justify-center ${
                showFloatingFilters
                  ? "border-[#004832] text-[#004832]"
                  : "border-gray-300 text-gray-700"
              }`}
              onClick={() => setShowFloatingFilters((v) => !v)}
            >
              <i className="ph ph-funnel" />
            </button>
          </div>

          {errorText && (
            <div className="mt-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-2">
              {errorText}
            </div>
          )}

          {hasData && (
            <Box
              className="ag-theme-alpine rounded-lg shadow-md"
              style={{ height: "calc(100vh - 180px)", width: "100%" }}
            >
              <AgGridReact
                rowData={normRows}
                columnDefs={columnDefs}
                defaultColDef={{
                  flex: 1,
                  minWidth: 130,
                  sortable: true,
                  filter: true,
                  resizable: true,
                  enableValue: true,
                  enableRowGroup: true,
                  enablePivot: true,
                  enableCharts: true,
                  floatingFilter: showFloatingFilters
                }}
                autoGroupColumnDef={{
                  headerName: "Group",
                  minWidth: 260,
                  pinned: "left"
                }}
                sideBar={false}
                pivotMode={false}
                animateRows={true}
                enableRangeSelection={true}
                getContextMenuItems={(params) => {
                  const def = params.defaultItems;
                  const custom = ["chartRange", "pivotChart"];
                  return [...def, "separator", ...custom];
                }}
              />
            </Box>
          )}
        </form>
      </FormProvider>

      <Box className="pt-3">
        {loading ? <Skeleton height="520px" rounded="md" /> : null}
      </Box>
    </Box>
  );
}
