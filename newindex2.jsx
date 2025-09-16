// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider } from "react-hook-form";
import { useEffect, useMemo, useState } from "react";
import {
  DropdownFieldSet,
  InputFieldset,
  AppButton,
  Loader,
  Toggle,
  AgGridTable
} from "../../elements";
import { useFetch } from "../../../api";
import { Box, Collapsible, Skeleton } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const ENDPOINT = "/dq/combined";

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) =>
  rows.every((r) => r == null || r[k] == null || r[k] === "");

function buildColumnDefs(rows) {
  if (!rows?.length) return [];
  const keys = new Set();
  rows.forEach((r) => Object.keys(r || {}).forEach((k) => keys.add(k)));
  const visible = Array.from(keys).filter((k) => !isAllNullOrEmpty(rows, k));
  return visible.map((k) => ({
    headerName: prettify(k),
    field: k,
    sortable: true,
    filter: true,
    resizable: true,
    minWidth: /^\d{4}$/.test(k) ? 90 : 140
  }));
}

function buildHierarchy(rows) {
  if (!rows?.length) return { tree: {}, rules: {}, books: {}, hasRule: false, hasBook: false };
  const hasRule = rows.some((r) => r?.rule_type != null && r.rule_type !== "");
  const hasBook = rows.some((r) => r?.book != null && r.book !== "");

  const tree = {};
  const rules = {};
  const books = {};

  rows.forEach((r) => {
    const rt = hasRule ? String(r.rule_type ?? "—") : "All";
    const bk = hasBook ? String(r.book ?? "—") : "All";
    if (!tree[rt]) tree[rt] = {};
    if (!tree[rt][bk]) tree[rt][bk] = true;
    rules[rt] = (rules[rt] || 0) + 1;
    books[`${rt}||${bk}`] = (books[`${rt}||${bk}`] || 0) + 1;
  });

  return { tree, rules, books, hasRule, hasBook };
}

export default function CosmosReports() {
  const methods = useForm();
  const { setValue, getValues } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const [filtersOpen, setFiltersOpen] = useState(true);
  const [ruleOpenMap, setRuleOpenMap] = useState({});
  const [activeRule, setActiveRule] = useState(null);
  const [activeBook, setActiveBook] = useState(null);

  const { tree, rules, books, hasRule, hasBook } = useMemo(
    () => buildHierarchy(rows),
    [rows]
  );

  const filteredRows = useMemo(() => {
    if (!rows?.length) return [];
    return rows.filter((r) => {
      if (activeRule != null && hasRule) {
        if (String(r.rule_type ?? "—") !== String(activeRule)) return false;
      }
      if (activeBook != null && hasBook) {
        if (String(r.book ?? "—") !== String(activeBook)) return false;
      }
      return true;
    });
  }, [rows, activeRule, activeBook, hasRule, hasBook]);

  const columnDefs = useMemo(() => buildColumnDefs(rows), [rows]);

  const onRun = async () => {
    const d = getValues("report_date");
    setActiveRule(null);
    setActiveBook(null);
    setRuleOpenMap({});
    if (!d) {
      setRows([]);
      return;
    }
    setLoading(true);
    const data = await useFetch(`${ENDPOINT}?report_date=${encodeURIComponent(d)}&limit=500`);
    const out = Array.isArray(data) ? data : Array.isArray(data?.rows) ? data.rows : [];
    setRows(out || []);
    setLoading(false);
  };

  const onReset = () => {
    setValue("report_date", "");
    setRows([]);
    setActiveRule(null);
    setActiveBook(null);
    setRuleOpenMap({});
  };

  const hasData = filteredRows.length > 0;

  return (
    <Box className="overflow-auto" height="calc(100vh - 70px)">
      <FormProvider {...methods}>
        <form className="flex flex-col gap-3">
          <div className="p-4 bg-white shadow-md rounded-lg" style={{ marginTop: -16 }}>
            <div className="flex gap-4 pb-3">
              <span className="text-lg font-bold">DQ Reports</span>
            </div>

            <div className="flex gap-4 pb-3">
              <InputFieldset
                id="report_date"
                label="Report Date"
                fieldName="report_date"
                tooltipMsg="Report Date"
                type="date"
              />
            </div>

            <div className="flex justify-end gap-4">
              <AppButton name="action" value="RESET" variant="secondary" onClick={onReset}>
                Reset
              </AppButton>
              <AppButton name="action" value="RUN" onClick={onRun}>
                Run
              </AppButton>
            </div>

            {rows.length > 0 && (hasRule || hasBook) && (
              <>
                <div className="flex gap-2 items-center pt-4 pb-2">
                  <a
                    className="text-sm text-blue-800 cursor-pointer"
                    onClick={() => setFiltersOpen(!filtersOpen)}
                  >
                    <span className="underline">Filters</span>
                  </a>
                  <i
                    className={`transition-transform duration-300 ph ph-caret-down ${
                      filtersOpen ? "rotate-180" : "rotate-0"
                    }`}
                  />
                  {(activeRule || activeBook) && (
                    <a
                      className="text-sm text-blue-800 cursor-pointer ml-4"
                      onClick={() => {
                        setActiveRule(null);
                        setActiveBook(null);
                      }}
                    >
                      Clear selection
                    </a>
                  )}
                </div>

                <Collapsible.Root open={filtersOpen} unmountOnExit>
                  <Collapsible.Content>
                    <div className="flex flex-col gap-2">
                      {Object.keys(tree)
                        .sort()
                        .map((rt) => {
                          const open = !!ruleOpenMap[rt];
                          const ruleCount = rules[rt] || 0;
                          const booksForRule = Object.keys(tree[rt] || {}).sort();
                          return (
                            <div key={rt} className="border rounded-md bg-gray-50">
                              <div className="flex items-center justify-between px-3 py-2">
                                <div
                                  className="flex items-center gap-2 cursor-pointer"
                                  onClick={() =>
                                    setRuleOpenMap((s) => ({ ...s, [rt]: !open }))
                                  }
                                >
                                  <i
                                    className={`ph ${
                                      open ? "ph-caret-down" : "ph-caret-right"
                                    }`}
                                  />
                                  <span
                                    className={`${
                                      String(activeRule) === rt && !activeBook
                                        ? "font-semibold"
                                        : ""
                                    }`}
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setActiveRule((prev) =>
                                        prev === rt && !activeBook ? null : rt
                                      );
                                      setActiveBook(null);
                                    }}
                                  >
                                    {rt}
                                  </span>
                                  <span className="text-xs text-gray-600">{ruleCount}</span>
                                </div>
                              </div>

                              {open && (
                                <div className="flex flex-col pb-2">
                                  {booksForRule.map((bk) => {
                                    const count = books[`${rt}||${bk}`] || 0;
                                    const selected =
                                      String(activeRule) === rt &&
                                      String(activeBook) === bk;
                                    return (
                                      <div
                                        key={`${rt}||${bk}`}
                                        className="px-8 py-1.5 hover:bg-gray-100 cursor-pointer"
                                        onClick={() =>
                                          setActiveBook((prev) =>
                                            selected ? null : bk
                                          )
                                        }
                                      >
                                        <span
                                          className={`text-sm ${
                                            selected ? "font-semibold" : ""
                                          }`}
                                        >
                                          {bk}
                                        </span>
                                        <span className="ml-2 text-xs text-gray-600">
                                          {count}
                                        </span>
                                      </div>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                          );
                        })}
                    </div>
                  </Collapsible.Content>
                </Collapsible.Root>
              </>
            )}
          </div>
        </form>
      </FormProvider>

      <Box className="pt-3">
        {loading ? (
          <Skeleton height="520px" rounded="md" />
        ) : hasData ? (
          <Box
            style={{ height: "600px", width: "100%" }}
            className="ag-theme-alpine rounded-lg shadow-md pt-3"
          >
            <AgGridReact
              rowData={filteredRows}
              columnDefs={columnDefs}
              defaultColDef={{
                flex: 1,
                minWidth: 130,
                enableValue: true,
                enableRowGroup: true,
                enablePivot: true,
                enableCharts: true
              }}
              autoGroupColumnDef={{ minWidth: 200, pinned: "left" }}
              sideBar={"columns"}
              pivotMode={false}
              enableRangeSelection={true}
              animateRows={true}
              getContextMenuItems={(params) => {
                const def = params.defaultItems;
                const custom = ["chartRange", "pivotChart"];
                return [...def, "separator", ...custom];
              }}
            />
          </Box>
        ) : null}
      </Box>
    </Box>
  );
}
