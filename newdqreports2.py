from typing import Any, Dict, List, Optional
from datetime import date
import pandas as pd
from core.db import DBConnection

CATALOG = "niwa_dev.gold"

VIEW_BY_REPORT: Dict[str, str] = {
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
        """
        Be permissive with date columns + formats:
        - columns: report_date | as_of_date | as_of_dt
        - formats: 'YYYY-MM-DD' and 'YYYYMMDD'
        """
        if report_date:
            d_dash = report_date.strftime("%Y-%m-%d")   # 2025-04-04
            return f"""
                SELECT * FROM {view}
                WHERE
                      to_date(CAST(report_date AS STRING))                 = to_date('{d_dash}')
                   OR to_date(CAST(report_date AS STRING), 'yyyyMMdd')     = to_date('{d_dash}')
                   OR to_date(CAST(as_of_date  AS STRING))                 = to_date('{d_dash}')
                   OR to_date(CAST(as_of_date  AS STRING), 'yyyyMMdd')     = to_date('{d_dash}')
                   OR to_date(CAST(as_of_dt    AS STRING))                 = to_date('{d_dash}')
                   OR to_date(CAST(as_of_dt    AS STRING), 'yyyyMMdd')     = to_date('{d_dash}')
                ORDER BY coalesce(
                    to_timestamp(CAST(report_date AS STRING)),
                    to_timestamp(CAST(as_of_date  AS STRING)),
                    to_timestamp(CAST(as_of_dt    AS STRING))
                ) DESC
                LIMIT {limit}
            """
        else:
            return f"""
                SELECT * FROM {view}
                ORDER BY coalesce(
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
        with DBConnection() as db:
            for rpt, view in VIEW_BY_REPORT.items():
                q = DQReports._sql(view, report_date, limit)
                df = db.execute(q, df=True)
                if df is None or df.empty:
                    continue
                df = DQReports._normalize(df)
                df.insert(0, "report_type", rpt)  # tag the source
                frames.append(df)

        if not frames:
            return []

        out = pd.concat(frames, ignore_index=True)
        return out.to_dict(orient="records")
