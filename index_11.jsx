// frontend/src/components/pages/CosmosReports/index.jsx
import { useForm, FormProvider, useWatch } from "react-hook-form";
import { useEffect, useMemo, useRef, useState } from "react";
import { InputFieldset } from "../../elements";
import { Box } from "@chakra-ui/react";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-enterprise";

const API_ENDPOINT = "/api/dq/combined";
const MIN_YEAR = 2021;

const prettify = (k) => k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const isNilOrEmpty = (v) => v === null || v === undefined || v === "";

const DETAIL_FIELDS = [
  { header: "Unique Tag",        keys: ["unique_tag", "unique tag", "uniquetag"] },
  { header: "Risk Factor Value", keys: ["risk_factor_value", "risk factor value", "rf_value", "value"] },
  { header: "Mean Value",        keys: ["mean_value", "mean value", "mean"] },
  { header: "Z Score",           keys: ["z score", "z_score", "zscore"] },
  { header: "Std Value",         keys: ["std value", "std_value", "std", "stddev", "std_dev"] },
  { header: "Is Outlier",        keys: ["is outlier", "is_outlier", "outlier"] },
];

const lc = (s) => String(s || "").toLowerCase();

function pickKey(row, candidates) {
  if (!row) return null;
  const candSet = new Set(candidates.map(lc));
  for (const k of Object.keys(row)) if (candSet.has(lc(k))) return k;
  return null;
}

function findKeyInRows(rows, candidates) {
  if (!rows?.length) return null;
  for (const r of rows) {
    const k = pickKey(r, candidates);
    if (k) return k;
  }
  return null;
}

function getYearKeys(rows) {
  if (!rows?.length) return ["2021", "2022", "2023", "2024", "2025"];
  const years = Array.from(
    new Set(
      rows.flatMap((r) =>
        Object.keys(r || {}).filter((k) => /^\d{4}$/.test(k) && Number(k) >= MIN_YEAR)
      )
    )
  ).sort
