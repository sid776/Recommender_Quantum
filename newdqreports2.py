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
                SELECT * FROM {view}
                WHERE to_date(CAST(report_date AS STRING)) = to_date('{d}')
                ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                LIMIT {limit}
            """
        return f"""
            SELECT * FROM {view}
            ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
            LIMIT {limit}
        """

    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
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
            for key, view in VIEW_BY_REPORT.items():
                q = DQReports._sql(view, report_date, limit)
                try:
                    part = db.execute(q, df=True)
                except Exception:
                    continue
                if isinstance(part, pd.DataFrame) and not part.empty:
                    part.insert(0, "report_type", key)  # e.g., 'summary', 'outliers'
                    frames.append(part)
        if not frames:
            return []
        df = DQReports._normalize(pd.concat(frames, ignore_index=True, sort=False))
        return df.to_dict(orient="records")
