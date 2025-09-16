// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider } from "react-hook-form";
import { useEffect, useMemo, useState } from "react";
import { InputFieldset, AppButton } from "../../elements";
import { Box, Skeleton } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const YEARS = ["2021", "2022", "2023", "2024", "2025"];

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => {
    const v = r?.[k];
    if (YEARS.includes(k)) return v == null || v === "" || Number(v) === 0;
    return v == null || v === "";
  });

const toYMD = (v) => {
  if (v == null) return "";
  const s = String(v).trim();
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return "";
};

function normalizeRows(rows) {
  const base = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  return (rows || []).map((r) => {
    const o = { ...r };
    base.forEach((k) => {
      if (!(k in o)) o[k] = YEARS.includes(k) ? 0 : "";
    });
    if (o.report_date) o.report_date = toYMD(o.report_date) || o.report_date;
    YEARS.forEach((y) => {
      const v = o[y];
      o[y] = typeof v === "number" ? v : v == null || v === "" ? 0 : Number(v);
    });
    return o;
  });
}

function buildColumnDefs(rows) {
  const ordered = ["report_date", "risk_factor_id", "rule_type", "book", ...YEARS];
  const present = rows?.length
    ? Array.from(new Set(rows.flatMap((r) => Object.keys(r))))
    : [];
  const extras = present.filter((k) => !ordered.includes(k));
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
        minWidth: YEARS.includes(k) ? 90 : 140
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

  const normRows = useMemo(() => normalizeRows(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(normRows), [normRows]);
  const hasData = normRows.length > 0;

  // initial load: infer latest date, set control, then load for that date
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoading(true);
        const r0 = await fetch(`${API_ENDPOINT}?limit=1`, { headers: { Accept: "application/json" } });
        const j0 = (await r0.json().catch(() => [])) || [];
        const first = Array.isArray(j0) ? j0[0] : Array.isArray(j0?.rows) ? j0.rows[0] : null;
        const inferred = first?.report_date ? toYMD(first.report_date) || first.report_date : "";
        if (!cancelled && inferred) {
          setValue("report_date", inferred);
          const r1 = await fetch(`${API_ENDPOINT}?report_date=${encodeURIComponent(inferred)}&limit=500`, {
            headers: { Accept: "application/json" }
          });
          const j1 = await r1.json().catch(() => []);
          const data = Array.isArray(j1) ? j1 : Array.isArray(j1?.rows) ? j1.rows : [];
          if (!cancelled) setRows(data || []);
        }
      } catch {
        if (!cancelled) setRows([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [setValue]);

  async function onRun() {
    setErrorText("");
    const d = getValues("report_date");
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

  function onReset() {
    setValue("report_date", "");
    setRows([]);
    setErrorText("");
  }

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            <div className="flex gap-4 pb-3">
              <span className="text-lg font-bold">DQ Reports</span>
            </div>

            <div className="flex gap-4 pb-3 items-end">
              <InputFieldset
                id="report_date"
                label="Report Date"
                fieldName="report_date"
                tooltipMsg="Report Date"
                type="date"
              />
              <div className="flex gap-3 ml-auto">
                <AppButton name="action" value="RESET" variant="secondary" onClick={onReset}>
                  Reset
                </AppButton>
                <AppButton name="action" value="RUN" onClick={onRun}>
                  Run
                </AppButton>
              </div>
            </div>

            {errorText && (
              <div className="mt-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-2">
                {errorText}
              </div>
            )}

            <div className="mt-2">
              <div className="flex items-center justify-between px-1 py-2">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">Combined DQ Reports</span>
                  {hasData ? (
                    <span className="text-xs text-gray-600">{normRows.length}</span>
                  ) : null}
                </div>
              </div>

              <Box
                className="ag-theme-alpine rounded-lg shadow-md"
                style={{ height: "600px", width: "100%" }}
              >
                {loading ? (
                  <Skeleton height="100%" rounded="md" />
                ) : hasData ? (
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
                      enableCharts: true
                    }}
                    suppressAggFuncInHeader={true}
                    autoGroupColumnDef={{ headerName: "Group", minWidth: 260, pinned: "left" }}
                    groupDisplayType="multipleColumns"
                    sideBar={{
                      position: "right",
                      defaultToolPanel: null,
                      toolPanels: [
                        {
                          id: "columns",
                          labelDefault: "Columns",
                          iconKey: "columns",
                          toolPanel: "agColumnsToolPanel",
                          toolPanelParams: { suppressPivotMode: true }
                        },
                        {
                          id: "filters",
                          labelDefault: "Filter",
                          iconKey: "filter",
                          toolPanel: "agFiltersToolPanel"
                        }
                      ]
                    }}
                    animateRows={true}
                    enableRangeSelection={true}
                    getContextMenuItems={(params) => {
                      const def = params.defaultItems;
                      const custom = ["chartRange", "pivotChart"];
                      return [...def, "separator", ...custom];
                    }}
                  />
                ) : null}
              </Box>
            </div>
          </div>
        </form>
      </FormProvider>
    </Box>
  );
}
