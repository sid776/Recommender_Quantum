# backend/objects/dq_reports.py
from typing import Any, Dict, List, Optional
from datetime import date
import pandas as pd

from core.db import DBConnection

CATALOG = "niwa_dev.gold"

VIEW_BY_REPORT = {
    "summary":       f"{CATALOG}.vw_smbc_marx_validation_summary_report",
    "staleness":     f"{CATALOG}.vw_smbc_marx_validation_staleness_report",
    "outliers":      f"{CATALOG}.vw_smbc_marx_validation_outlier_report",
    "availability":  f"{CATALOG}.vw_smbc_marx_validation_availability_report",
    "reasonability": f"{CATALOG}.vw_smbc_marx_validation_reasonability_report",
    "schema":        f"{CATALOG}.vw_smbc_marx_validation_schema_report",
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
        if df.empty:
            return df

        # unify a couple of column names if present
        renames: Dict[str, str] = {}
        if "table" in df.columns and "table_name" not in df.columns:
            renames["table"] = "table_name"
        if renames:
            df = df.rename(columns=renames)

        # ensure group columns exist
        for c in GROUP_COLS:
            if c not in df.columns:
                df[c] = None

        return df

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        frames: List[pd.DataFrame] = []
        with DBConnection() as db:
            for view in VIEW_BY_REPORT.values():
                q = DQReports._sql(view, report_date, limit)
                part = db.execute(q, df=True)
                if not isinstance(part, pd.DataFrame):
                    continue
                # tag the source so FE can distinguish if needed
                part.insert(0, "report_type", view.split("_")[-2] if "validation_" in view else view)
                frames.append(part)

        if not frames:
            return []

        df = pd.concat(frames, ignore_index=True, sort=False)
        df = DQReports._normalize(df)
        return df.to_dict(orient="records")
