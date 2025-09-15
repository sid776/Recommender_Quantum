# backend/objects/dq_reports.py
from typing import Any, Dict, List, Optional
from datetime import date
import pandas as pd

from core.db import DBConnection   # your existing helper

CATALOG = "niwa_dev.gold"

VIEW_BY_REPORT = {
    "summary":      f"{CATALOG}.vw_smbc_marx_validation_summary_report",
    "staleness":    f"{CATALOG}.vw_smbc_marx_validation_staleness_report",
    "outliers":     f"{CATALOG}.vw_smbc_marx_validation_outlier_report",
    "availability": f"{CATALOG}.vw_smbc_marx_validation_availability_report",
    "reasonability":f"{CATALOG}.vw_smbc_marx_validation_reasonability_report",
    "schema":       f"{CATALOG}.vw_smbc_marx_validation_schema_report",
}

# Columns that commonly exist across the views.
# We’ll use them to drive grouping on the FE if present.
GROUP_COLS = ["rule_type", "book"]

class DQReports:
    @staticmethod
    def _sql(view: str, report_date: Optional[date], limit: int) -> str:
        """
        Filter by report_date if the view has it, otherwise by as_of_date.
        We do it with COALESCE-like OR filters so one query works everywhere.
        Handles 'YYYYMMDD' and 'YYYY-MM-DD' strings in the lake views.
        """
        if report_date:
            d = report_date.strftime("%Y-%m-%d")
            return f"""
                SELECT *
                FROM {view}
                WHERE
                      to_date(CAST(report_date AS STRING)) = to_date('{d}')
                   OR to_date(CAST(as_of_date  AS STRING)) = to_date('{d}')
                   OR to_date(CAST(as_of_dt    AS STRING)) = to_date('{d}')
                ORDER BY
                    coalesce(
                        to_timestamp(CAST(report_date AS STRING)),
                        to_timestamp(CAST(as_of_date  AS STRING)),
                        to_timestamp(CAST(as_of_dt    AS STRING))
                    ) DESC
                LIMIT {limit}
            """
        else:
            return f"""
                SELECT *
                FROM {view}
                ORDER BY
                    coalesce(
                        to_timestamp(CAST(report_date AS STRING)),
                        to_timestamp(CAST(as_of_date  AS STRING)),
                        to_timestamp(CAST(as_of_dt    AS STRING))
                    ) DESC
                LIMIT {limit}
            """

    @staticmethod
    def get(report: str, report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        view = VIEW_BY_REPORT.get((report or "").lower())
        if not view:
            return []

        with DBConnection() as db:
            q = DQReports._sql(view, report_date, limit)
            df: pd.DataFrame = db.execute(q, df=True)

        # Normalize common column names so FE grouping is consistent
        # (only remap if columns exist)
        renames = {}
        if "table" in df.columns and "table_name" not in df.columns:
            renames["table"] = "table_name"
        if "partition" in df.columns and "partition_dt" in df.columns:
            # prefer clearer name if both exist
            pass

        if renames:
            df = df.rename(columns=renames)

        # Ensure the group columns exist with None if missing
        for c in GROUP_COLS:
            if c not in df.columns:
                df[c] = None

        return df.to_dict(orient="records")
