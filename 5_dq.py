from typing import Any, Dict, List, Optional, Tuple
from datetime import date
import pandas as pd

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

def _to_records(df) -> List[_ReportRow]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")
    return []

def _fallback_sql(view: str, report_date: Optional[date], limit: int) -> str:
    if report_date:
        d = report_date.strftime("%Y-%m-%d")
        return f"""
            SELECT *
            FROM {view}
            WHERE COALESCE(
                CAST(report_date AS DATE),
                TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'),
                TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd')
            ) = DATE'{d}'
            ORDER BY COALESCE(
                CAST(report_date AS DATE),
                TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'),
                TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd')
            ) DESC
            LIMIT {limit}
        """
    else:
        return f"""
            SELECT *
            FROM {view}
            ORDER BY COALESCE(
                CAST(report_date AS DATE),
                TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'),
                TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd')
            ) DESC
            LIMIT {limit}
        """

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

def _fetch_section(name: str, obj_cls: type, report_date: Optional[date], limit: int) -> List[_ReportRow]:
    # try object model first
    try:
        fn = getattr(obj_cls, "get_dataframe", None)
        if callable(fn):
            df = fn(report_date=report_date, limit=limit, pyspark=False)
            recs = _to_records(df)
            if recs:
                for r in recs:
                    r.setdefault("report_type", name)
                return recs
    except Exception:
        pass

    # fallback: run SQL directly
    try:
        from core.db import DBConnection
        sql = _fallback_sql(VIEW_BY_REPORT[name], report_date, limit)
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        if df is not None and not df.empty:
            return _normalize(df, name).to_dict(orient="records")
    except Exception:
        return []
    return []

class DQReports:
    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        return _fetch_section("summary", DQSummary, report_date, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        return _fetch_section("staleness", DQStaleness, report_date, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        return _fetch_section("outliers", DQOutliers, report_date, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        return _fetch_section("availability", DQAvailability, report_date, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        return _fetch_section("reasonability", DQReasonability, report_date, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        return _fetch_section("schema", DQSchema, report_date, limit)

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        sections: List[Tuple[str, Any]] = [
            ("summary",       DQReports.get_summary),
            ("staleness",     DQReports.get_staleness),
            ("outliers",      DQReports.get_outliers),
            ("availability",  DQReports.get_availability),
            ("reasonability", DQReports.get_reasonability),
            ("schema",        DQReports.get_schema),
        ]
        out: List[_ReportRow] = []
        for name, getter in sections:
            rows = getter(report_date=report_date, limit=limit)
            if rows:
                out.extend(rows)
        return out
