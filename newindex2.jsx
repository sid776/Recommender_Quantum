// frontend/src/components/pages/CosmosReports/index.jsx
import React, { useEffect, useMemo, useState } from "react";
import {
  Box,
  HStack,
  Button,
  Skeleton,
  Text,
  Grid,
  GridItem,
  VStack,
  IconButton,
  Divider,
  Badge,
} from "@chakra-ui/react";
import { ChevronDownIcon, ChevronRightIcon, CloseIcon } from "@chakra-ui/icons";
import { useForm, FormProvider } from "react-hook-form";
import InputFieldSet from "../../elements/InputFieldset.jsx";
import AgGridTable from "../../elements/AgGridTable.jsx";

const ENDPOINT = "/api/dq/combined";

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isAllNullOrEmpty = (rows, k) => rows.every((r) => r == null || r[k] == null || r[k] === "");

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
    minWidth: /^\d{4}$/.test(k) ? 90 : 140,
    valueFormatter: (p) =>
      k === "report_date" && typeof p.value === "string" && /^\d{8}$/.test(p.value)
        ? `${p.value.slice(0, 4)}-${p.value.slice(4, 6)}-${p.value.slice(6, 8)}`
        : p.value,
  }));
}

function buildHierarchy(rows) {
  if (!rows?.length) return { tree: {}, counts: {}, hasRule: false, hasBook: false };

  const hasRule = rows.some((r) => r?.rule_type != null && r.rule_type !== "");
  const hasBook = rows.some((r) => r?.book != null && r.book !== "");
  if (!hasRule && !hasBook) return { tree: {}, counts: {}, hasRule, hasBook };

  const tree = {};
  const counts = { rules: {}, books: {} };

  rows.forEach((r) => {
    const rt = hasRule ? String(r.rule_type ?? "—") : "All";
    const bk = hasBook ? String(r.book ?? "—") : "All";
    if (!tree[rt]) tree[rt] = {};
    if (!tree[rt][bk]) tree[rt][bk] = true;
    counts.rules[rt] = (counts.rules[rt] || 0) + 1;
    counts.books[`${rt}||${bk}`] = (counts.books[`${rt}||${bk}`] || 0) + 1;
  });

  return { tree, counts, hasRule, hasBook };
}

export default function CosmosReports() {
  const methods = useForm({ defaultValues: { reportDate: "" } });
  const { setValue, getValues } = methods;

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [menuSpace, setMenuSpace] = useState(false);

  const [openRules, setOpenRules] = useState({});
  const [activeRule, setActiveRule] = useState(null);
  const [activeBook, setActiveBook] = useState(null);

  useEffect(() => {
    // clear selections when date is cleared
    if (!getValues("reportDate")) {
      setRows([]);
      setActiveRule(null);
      setActiveBook(null);
      setOpenRules({});
    }
  }, [methods, getValues]);

  async function run() {
    setLoading(true);
    try {
      const reportDate = getValues("reportDate");
      if (!reportDate) {
        setRows([]);
        return;
      }
      const url = new URL(ENDPOINT, window.location.origin);
      url.searchParams.set("report_date", reportDate);
      url.searchParams.set("limit", "500");

      const res = await fetch(url.toString());
      const json = await res.json();
      const data = Array.isArray(json) ? json : [];
      setRows(data);

      const { tree } = buildHierarchy(data);
      const firstRule = Object.keys(tree || {})[0] || null;
      setOpenRules(firstRule ? { [firstRule]: true } : {});
      setActiveRule(null);
      setActiveBook(null);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  const { tree, counts, hasRule, hasBook } = useMemo(() => buildHierarchy(rows), [rows]);
  const columnDefs = useMemo(() => buildColumnDefs(rows), [rows]);

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

  const hasData = filteredRows.length > 0;

  return (
    <FormProvider {...methods}>
      <Box className="mx-auto max-w-[1400px] p-4" style={{ overflow: "visible" }}>
        <Box
          className="bg-white rounded-lg shadow-lg p-6"
          style={{ position: "relative", zIndex: 0, overflow: "visible", marginTop: -16 }}
        >
          <Text fontSize="lg" fontWeight="bold" mb={4}>
            DQ Reports
          </Text>

          <Grid
            templateColumns="1fr 1fr"
            gap="8px 24px"
            alignItems="end"
            style={{ overflow: "visible", width: "100%" }}
            onFocusCapture={() => setMenuSpace(true)}
            onBlurCapture={() => setMenuSpace(false)}
          >
            <GridItem>
              <Text fontSize="sm" color="gray.700" fontWeight="semibold">
                Report Date
              </Text>
            </GridItem>
            <GridItem />{/* empty cell to keep two-column layout */}

            <GridItem minW="320px" style={{ display: "flex", alignItems: "center" }}>
              <InputFieldSet id="reportDate" fieldName="reportDate" type="date" />
            </GridItem>

            <GridItem colSpan={2}>
              <HStack justify="flex-end" spacing={3} mt={2}>
                <Button
                  size="sm"
                  variant="outline"
                  borderColor="#0f5c2e"
                  color="#0f5c2e"
                  onClick={() => {
                    setRows([]);
                    setValue("reportDate", "");
                    setActiveRule(null);
                    setActiveBook(null);
                    setOpenRules({});
                  }}
                >
                  Reset
                </Button>
                <Button
                  size="sm"
                  bg="#0f5c2e"
                  color="white"
                  _hover={{ bg: "#0d4f27" }}
                  onClick={run}
                  isLoading={loading}
                >
                  Run
                </Button>
              </HStack>
            </GridItem>
          </Grid>

          {(hasRule || hasBook) && rows.length > 0 ? (
            <Box mt={4} p={3} borderWidth="1px" borderRadius="md" bg="gray.50">
              <HStack justify="space-between" mb={2}>
                <Text fontWeight="semibold">Filter</Text>
                {(activeRule || activeBook) ? (
                  <Button
                    size="xs"
                    variant="ghost"
                    leftIcon={<CloseIcon boxSize="0.6em" />}
                    onClick={() => {
                      setActiveRule(null);
                      setActiveBook(null);
                    }}
                  >
                    Clear selection
                  </Button>
                ) : null}
              </HStack>
              <Divider mb={2} />
              <VStack align="stretch" spacing={1}>
                {Object.keys(tree).sort().map((rt) => {
                  const isOpen = !!openRules[rt];
                  const books = Object.keys(tree[rt] || {}).sort();
                  const ruleCount = counts.rules[rt] || 0;

                  return (
                    <Box key={rt} bg="white" borderWidth="1px" borderRadius="md">
                      <HStack px={2} py={1.5} justify="space-between">
                        <HStack spacing={2}>
                          <IconButton
                            size="xs"
                            aria-label={isOpen ? "Collapse" : "Expand"}
                            icon={isOpen ? <ChevronDownIcon /> : <ChevronRightIcon />}
                            onClick={() => setOpenRules((s) => ({ ...s, [rt]: !isOpen }))}
                          />
                          <Text
                            fontWeight={String(activeRule) === rt && !activeBook ? "bold" : "normal"}
                            cursor="pointer"
                            onClick={() => {
                              setActiveRule((prev) => (prev === rt && !activeBook ? null : rt));
                              setActiveBook(null);
                            }}
                          >
                            {rt}
                          </Text>
                          <Badge>{ruleCount}</Badge>
                        </HStack>
                      </HStack>

                      {isOpen ? (
                        <VStack align="stretch" spacing={0} pb={1}>
                          {books.map((bk) => {
                            const count = counts.books[`${rt}||${bk}`] || 0;
                            const selected = String(activeRule) === rt && String(activeBook) === bk;
                            return (
                              <HStack
                                key={`${rt}||${bk}`}
                                px={8}
                                py={1.5}
                                _hover={{ bg: "gray.50" }}
                                cursor="pointer"
                                onClick={() => {
                                  setActiveRule(rt);
                                  setActiveBook((prev) => (selected ? null : bk));
                                }}
                              >
                                <Text fontSize="sm" fontWeight={selected ? "bold" : "normal"}>
                                  {bk}
                                </Text>
                                <Badge>{count}</Badge>
                              </HStack>
                            );
                          })}
                        </VStack>
                      ) : null}
                    </Box>
                  );
                })}
              </VStack>
            </Box>
          ) : null}

          <Box style={{ height: menuSpace ? "0px" : 0, transition: "height 120ms" }} />

          {loading ? (
            <Box mt={2}>
              <Skeleton height="520px" rounded="md" />
            </Box>
          ) : hasData ? (
            <Box mt={2} style={{ height: "calc(100vh - 420px)" }}>
              <AgGridTable
                rowData={filteredRows}
                columnDefs={columnDefs}
                pagination={true}
                paginationPageSize={50}
                defaultColDef={{ sortable: true, filter: true, resizable: true }}
                animateRows={true}
                style={{ height: "100%" }}
              />
            </Box>
          ) : null}
        </Box>
      </Box>
    </FormProvider>
  );
}
