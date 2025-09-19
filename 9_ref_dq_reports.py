# dq_reports.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import date
import pandas as pd

from core.db import DBConnection

_ReportRow = Dict[str, Any]

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
        norm_expr = """
            COALESCE(
                CAST(report_date AS DATE),
                TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'),
                TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd')
            )
        """
        if report_date:
            d = report_date.strftime("%Y-%m-%d")
            return f"""
                SELECT *
                FROM {view}
                WHERE {norm_expr} = DATE'{d}'
                ORDER BY {norm_expr} DESC
                LIMIT {limit}
            """
        return f"""
            SELECT *
            FROM {view}
            ORDER BY {norm_expr} DESC
            LIMIT {limit}
        """

    @staticmethod
    def _normalize(df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        renames: Dict[str, str] = {}
        if "table" in df.columns and "table_name" not in df.columns:
            renames["table"] = "table_name"
        if renames:
            df = df.rename(columns=renames)
        for c in GROUP_COLS:
            if c not in df.columns:
                df[c] = None
        if "report_type" not in df.columns:
            df.insert(0, "report_type", report_type)
        return df

    @staticmethod
    def _fetch_section(key: str, obj_cls: Optional[type], report_date: Optional[date], limit: int) -> pd.DataFrame:
        # Try object model first
        if obj_cls is not None:
            try:
                df = obj_cls.get_dataframe(report_date=report_date, limit=limit, pyspark=False)
                if df is not None and not df.empty:
                    df = DQReports._normalize(df, key)
                    return df
            except Exception:
                pass  # fallback to SQL

        # Fallback: SQL query
        try:
            with DBConnection() as db:
                q = DQReports._sql(VIEW_BY_REPORT[key], report_date, limit)
                df = db.execute(q, df=True)
                if df is not None and not df.empty:
                    df = DQReports._normalize(df, key)
                    return df
        except Exception:
            return pd.DataFrame()
        return pd.DataFrame()

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.summary import DQSummary
        return DQReports._fetch_section("summary", DQSummary, report_date, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.staleness import DQStaleness
        return DQReports._fetch_section("staleness", DQStaleness, report_date, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.outliers import DQOutliers
        return DQReports._fetch_section("outliers", DQOutliers, report_date, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.availability import DQAvailability
        return DQReports._fetch_section("availability", DQAvailability, report_date, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.reasonability import DQReasonability
        return DQReports._fetch_section("reasonability", DQReasonability, report_date, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> pd.DataFrame:
        from objects.dq.schema import DQSchema
        return DQReports._fetch_section("schema", DQSchema, report_date, limit)

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        sections: List[Tuple[str, Any]] = [
            ("summary", DQReports.get_summary),
            ("staleness", DQReports.get_staleness),
            ("outliers", DQReports.get_outliers),
            ("availability", DQReports.get_availability),
            ("reasonability", DQReports.get_reasonability),
            ("schema", DQReports.get_schema),
        ]

        frames: List[pd.DataFrame] = []
        for name, getter in sections:
            df = getter(report_date=report_date, limit=limit)
            if df is not None and not df.empty:
                frames.append(df)

        if not frames:
            return []

        # Merge everything and group by the key columns
        merged = pd.concat(frames, ignore_index=True, sort=False)
        grouped = merged.groupby(GROUP_COLS, dropna=False).apply(lambda g: g.to_dict(orient="records")).reset_index()
        grouped.rename(columns={0: "rows"}, inplace=True)

        return grouped.to_dict(orient="records")
