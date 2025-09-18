from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import date

DATE_KEYS = ["report_date", "as_of_date", "cob_date", "cob_dt"]

def _yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _as_records(df_obj: Any) -> List[Dict[str, Any]]:
    if df_obj is None:
        return []
    to_dict = getattr(df_obj, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict(orient="records")  # type: ignore
        except TypeError:
            return to_dict()
    return []

def _debug_source(obj_cls: Any) -> None:
    schema = getattr(obj_cls, "TABLE_SCHEMA", "gold")
    table = getattr(obj_cls, "TABLE_NAME", "")
    print(f"Schema and Table/View being fetched from: niwa_dev.{schema}.{table}")

class DQReports:
    @staticmethod
    def _fetch_any(obj_cls: Any, report_date: Optional[date], limit: int, tag: str) -> List[Dict[str, Any]]:
        _debug_source(obj_cls)
        df = obj_cls.get_dataframe(limit=limit if limit and limit > 0 else 5000, pyspark=False)
        rows = _as_records(df)
        if not rows:
            return []

        if report_date is None:
            for r in rows:
                r.setdefault("report_type", tag)
            return rows

        target1 = _yyyy_mm_dd(report_date)   # "YYYY-MM-DD"
        target2 = _yyyymmdd(report_date)     # "YYYYMMDD"
        filtered: List[Dict[str, Any]] = []

        for r in rows:
            for key in DATE_KEYS:
                v = r.get(key)
                if not v:
                    continue
                s = str(v).strip()
                if s.startswith(target1) or s.startswith(target2):
                    r.setdefault("report_type", tag)
                    filtered.append(r)
                    break
        return filtered

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.summary import DQSummary
        return DQReports._fetch_any(DQSummary, report_date, limit, "summary")

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.staleness import DQStaleness
        return DQReports._fetch_any(DQStaleness, report_date, limit, "staleness")

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.outliers import DQOutliers
        return DQReports._fetch_any(DQOutliers, report_date, limit, "outliers")

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.availability import DQAvailability
        return DQReports._fetch_any(DQAvailability, report_date, limit, "availability")

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.reasonability import DQReasonability
        return DQReports._fetch_any(DQReasonability, report_date, limit, "reasonability")

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500):
        from objects.dq.schema import DQSchema
        return DQReports._fetch_any(DQSchema, report_date, limit, "schema")

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500):
        sections = [
            ("summary",       DQReports.get_summary),
            ("staleness",     DQReports.get_staleness),
            ("outliers",      DQReports.get_outliers),
            ("availability",  DQReports.get_availability),
            ("reasonability", DQReports.get_reasonability),
            ("schema",        DQReports.get_schema),
        ]
        out: List[Dict[str, Any]] = []
        for name, getter in sections:
            rows = getter(report_date=report_date, limit=limit)
            if rows:
                for r in rows:
                    r.setdefault("report_type", name)
                out.extend(rows)
        return out
