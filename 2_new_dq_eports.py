from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

_DATE_KEYS: Tuple[str, ...] = (
    "report_date",
    "cob_dt",
    "cob_date",
    "as_of_date",
    "date",
)

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def _yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def _normalize_row_date(row: Dict[str, Any]) -> Optional[str]:
    for k in _DATE_KEYS:
        if k in row and row[k] is not None and row[k] != "":
            v = row[k]
            if hasattr(v, "isoformat"):
                try:
                    return str(v.isoformat())[:10]
                except Exception:
                    pass
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                s = str(int(v))
                if len(s) == 8:
                    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
            if isinstance(v, str):
                s = v.strip()
                if len(s) == 10 and s[4] == "-" and s[7] == "-":
                    return s
                if len(s) == 8 and s.isdigit():
                    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None

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
        try:
            df = obj_cls.get_dataframe(limit=limit if limit and limit > 0 else 5000, pyspark=False)
        except TypeError:
            df = obj_cls.get_dataframe(pyspark=False)
        rows = _as_records(df)
        if not rows:
            return []
        if report_date is None:
            for r in rows:
                r.setdefault("report_type", tag)
            return rows
        target = _yyyy_mm_dd(report_date)
        target_alt = _yyyymmdd(report_date)
        filtered: List[Dict[str, Any]] = []
        for r in rows:
            d_norm = _normalize_row_date(r)
            if d_norm == target:
                r.setdefault("report_type", tag)
                filtered.append(r)
            else:
                for k in _DATE_KEYS:
                    v = r.get(k)
                    if isinstance(v, str) and v.strip() in (target, target_alt):
                        r.setdefault("report_type", tag)
                        filtered.append(r)
                        break
        return filtered

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.summary import DQSummary
        return DQReports._fetch_any(DQSummary, report_date, limit, "summary")

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.staleness import DQStaleness
        return DQReports._fetch_any(DQStaleness, report_date, limit, "staleness")

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.outliers import DQOutliers
        return DQReports._fetch_any(DQOutliers, report_date, limit, "outliers")

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.availability import DQAvailability
        return DQReports._fetch_any(DQAvailability, report_date, limit, "availability")

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.reasonability import DQReasonability
        return DQReports._fetch_any(DQReasonability, report_date, limit, "reasonability")

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.schema import DQSchema
        return DQReports._fetch_any(DQSchema, report_date, limit, "schema")

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
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
            try:
                rows = getter(report_date=report_date, limit=limit)
            except TypeError:
                rows = getter(report_date=report_date)
            if rows:
                for r in rows:
                    r.setdefault("report_type", name)
                out.extend(rows)
        return out
