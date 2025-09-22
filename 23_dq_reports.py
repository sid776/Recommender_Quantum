# dq_reports.py
from typing import Any, Dict, List, Optional
from datetime import date, datetime
import pandas as pd, os, inspect

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
USE_SQL_FALLBACK = os.getenv("USE_DQREPORTS_SQL_FALLBACK", "0").lower() in ("1","true","yes")
DATE_KWS = ("report_date","as_of_date","as_of_dt","cob_date")

class DQReports:
    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        renames: Dict[str,str] = {}
        if "table" in df.columns and "table_name" not in df.columns:
            renames["table"] = "table_name"
        if renames: df = df.rename(columns=renames)
        for c in GROUP_COLS:
            if c not in df.columns: df[c] = None
        return df

    @staticmethod
    def _sql(view: str, report_date: Optional[date], limit: int) -> str:
        n = "COALESCE(CAST(report_date AS DATE),TO_DATE(CAST(report_date AS STRING),'yyyyMMdd'),TO_DATE(CAST(report_date AS STRING),'yyyy-MM-dd'))"
        if report_date:
            d = report_date.strftime("%Y-%m-%d")
            return f"SELECT * FROM {view} WHERE {n}=DATE'{d}' ORDER BY {n} DESC LIMIT {limit}"
        return f"SELECT * FROM {view} ORDER BY {n} DESC LIMIT {limit}"

    @staticmethod
    def _latest_date() -> Optional[date]:
        n = "COALESCE(CAST(report_date AS DATE),TO_DATE(CAST(report_date AS STRING),'yyyyMMdd'),TO_DATE(CAST(report_date AS STRING),'yyyy-MM-dd'))"
        union = " UNION ALL ".join([f"SELECT MAX({n}) d FROM {v}" for v in VIEW_BY_REPORT.values()])
        sql = f"SELECT MAX(d) AS max_d FROM ({union}) t"
        try:
            with DBConnection() as db:
                df = db.execute(sql, df=True)
            if df is None or df.empty: return None
            val = df.iloc[0,0]
            if pd.isna(val): return None
            if isinstance(val, datetime): return val.date()
            if isinstance(val, date): return val
            try:
                return pd.to_datetime(val).date()
            except Exception:
                return None
        except Exception:
            return None

    @staticmethod
    def _fetch_section(key: str, obj_cls: Optional[type], report_date: Optional[date], limit: int):
        if obj_cls:
            try:
                fn = obj_cls.get_dataframe
                sig = inspect.signature(fn)
                common = dict(limit=limit, pyspark=False)
                if "date_formats" in sig.parameters:
                    common["date_formats"] = ["yyyyMMdd","yyyy-MM-dd"]
                if report_date:
                    for dk in DATE_KWS:
                        if dk in sig.parameters:
                            try:
                                df = fn(**{dk: report_date, **common})
                                if df is not None and not df.empty:
                                    recs = df.to_dict(orient="records")
                                    for r in recs: r.setdefault("report_type",key)
                                    return recs
                            except: pass
                df = fn(**common)
                if df is not None and not df.empty:
                    recs = df.to_dict(orient="records")
                    for r in recs: r.setdefault("report_type",key)
                    return recs
            except: pass
        if USE_SQL_FALLBACK:
            try:
                with DBConnection() as db:
                    df = db.execute(DQReports._sql(VIEW_BY_REPORT[key],report_date,limit),df=True)
                if df is not None and not df.empty:
                    df = DQReports._normalize(df)
                    df.insert(0,"report_type",key)
                    return df.to_dict(orient="records")
            except: pass
        return []

    @staticmethod
    def get_all(report_date: Optional[date]=None, limit: int=500) -> List[Dict[str,Any]]:
        if report_date is None:
            ld = DQReports._latest_date()
            if ld: report_date = ld

        from objects.dq.summary import DQSummary
        from objects.dq.staleness import DQStaleness
        from objects.dq.outliers import DQOutliers
        from objects.dq.availability import DQAvailability
        from objects.dq.reasonability import DQReasonability
        from objects.dq.schema import DQSchema

        sections = {
            "summary": DQSummary,
            "staleness": DQStaleness,
            "outliers": DQOutliers,
            "availability": DQAvailability,
            "reasonability": DQReasonability,
            "schema": DQSchema,
        }
        frames: List[Dict[str,Any]] = []
        for key,obj_cls in sections.items():
            rows = DQReports._fetch_section(key,obj_cls,report_date,limit)
            if rows: frames.extend(rows)
        return frames
