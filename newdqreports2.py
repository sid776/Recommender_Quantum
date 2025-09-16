# backend/objects/dq_reports.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import date
import pandas as pd

from core.db import DBConnection

CATALOG = "niwa_dev.gold"

# key -> (view, pretty label)
VIEW_BY_REPORT: Dict[str, Tuple[str, str]] = {
    "summary":       (f"{CATALOG}.vw_smbc_marx_validation_summary_report",       "DQ Summary"),
    "staleness":     (f"{CATALOG}.vw_smbc_marx_validation_staleness_report",     "DQ Staleness"),
    "outliers":      (f"{CATALOG}.vw_smbc_marx_validation_outlier_report",       "DQ Outliers"),
    "availability":  (f"{CATALOG}.vw_smbc_marx_validation_availability_report",  "DQ Availability"),
    "reasonability": (f"{CATALOG}.vw_smbc_marx_validation_reasonability_report", "DQ Reasonability"),
    "schema":        (f"{CATALOG}.vw_smbc_marx_validation_schema_report",        "DQ Schema"),
}

GROUP_COLS = ["rule_type", "book"]

class DQReports:
    @staticmethod
    def _sql(view: str, report_date: Optional[date], limit: int) -> str:
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
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        renames: Dict[str, str] = {}
        if "table" in df.columns and "table_name" not in df.columns:
            renames["table"] = "table_name"
        if renames:
            df = df.rename(columns=renames)
        for c in GROUP_COLS:
            if c not in df.columns:
                df[c] = None
        return df

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        frames: List[pd.DataFrame] = []
        # apply per-view cap so one view doesn’t starve others
        per_view_limit = max(1, limit)

        with DBConnection() as db:
            for key, (view, pretty) in VIEW_BY_REPORT.items():
                q = DQReports._sql(view, report_date, per_view_limit)
                df: pd.DataFrame = db.execute(q, df=True)
                if df is None or df.empty:
                    continue
                df = DQReports._normalize(df)
                df["report"] = pretty  # add a report label so FE can show/filter if needed
                frames.append(df)

        if not frames:
            return []

        all_df = pd.concat(frames, ignore_index=True, sort=False)
        return all_df.to_dict(orient="records")
