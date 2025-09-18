# backend/objects/dq_reports.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date, datetime

# ---- helpers ---------------------------------------------------------------

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
    """
    From a record (dict), find the first known date-like key and return a
    canonical 'YYYY-MM-DD' string for comparison.
    Supports values like date/datetime, 'YYYY-MM-DD', 'YYYYMMDD', or ints.
    """
    for k in _DATE_KEYS:
        if k in row and row[k] is not None and row[k] != "":
            v = row[k]
            # pandas Timestamp / datetime / date
            if hasattr(v, "isoformat"):
                try:
                    # pandas Timestamp.isoformat() returns full ts; slice date
                    return str(v.isoformat())[:10]
                except Exception:
                    pass
            # numeric like 20250917
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                s = str(int(v))
                if len(s) == 8:
                    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
            # strings
            if isinstance(v, str):
                s = v.strip()
                if len(s) == 10 and s[4] == "-" and s[7] == "-":
                    return s  # already YYYY-MM-DD
                if len(s) == 8 and s.isdigit():
                    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None

def _as_records(df_obj: Any) -> List[Dict[str, Any]]:
    """
    Convert an object (likely a pandas DataFrame) into list-of-dicts safely
    without importing pandas here.
    """
    if df_obj is None:
        return []
    to_dict = getattr(df_obj, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict(orient="records")  # type: ignore
        except TypeError:
            # some libs use different signature; fall back
            return to_dict()
    return []

def _debug_source(obj_cls: Any) -> None:
    """
    Print exactly one line with the fully-qualified source the object maps to,
    forcing the catalog to 'niwa_dev' per your environment.
    """
    schema = getattr(obj_cls, "TABLE_SCHEMA", "gold")
    table = getattr(obj_cls, "TABLE_NAME", "")
    print(f"Schema and Table/View being fetched from: niwa_dev.{schema}.{table}")

# ---- public API ------------------------------------------------------------

class DQReports:
    """
    Object-style façade that fans out to the individual DQ object models,
    normalizes dates, and returns list-of-dicts. No raw SQL.
    """

    @staticmethod
    def _fetch_any(
        obj_cls: Any,
        report_date: Optional[date],
        limit: int,
        tag: str,
    ) -> List[Dict[str, Any]]:
        """
        Call <Object>.get_dataframe(...) using enterprise-style kwargs,
        then filter rows by report_date in Python so we don't depend on the
        exact column name/format in each view.
        """
        _debug_source(obj_cls)

        # Try to ask the object for a generous page; we filter after.
        # Many enterprise objects accept 'order' and 'limit' kwargs.
        try:
            df = obj_cls.get_dataframe(limit=limit if limit and limit > 0 else 5000, pyspark=False)
        except TypeError:
            # if signature is stricter, try without limit
            df = obj_cls.get_dataframe(pyspark=False)

        rows = _as_records(df)
        if not rows:
            return []

        if report_date is None:
            # no filtering requested; just tag and return
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
                # occasionally rows store raw 8-digit strings and _normalize_row_date
                # may fail if the field is nested; as a safety, do a loose check:
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
        sections: List[Tuple[str, Any]] = [
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
                rows = getter(report_date=report_date)  # some wrappers only accept report_date
            if rows:
                # make sure report_type is present (safety if object already sets it)
                for r in rows:
                    r.setdefault("report_type", name)
                out.extend(rows)
        return out
