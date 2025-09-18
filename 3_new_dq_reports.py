# objects/dq_reports.py
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date, datetime
import inspect

_ReportRow = Dict[str, Any]
_DateKw = ("report_date", "cob_date", "as_of_date", "as_of_dt")

def _print_debug_object_source(obj_cls: type) -> None:
    schema = getattr(obj_cls, "TABLE_SCHEMA", "?")
    table  = getattr(obj_cls, "TABLE_NAME",  "?")
    print(f"[DQ] Reading from object {obj_cls.__name__} backing view/table -> {schema}.{table}")

def _supports_kw(obj_cls: type, name: str) -> bool:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return False
    sig = inspect.signature(fn)
    return name in sig.parameters

def _to_records(df) -> List[_ReportRow]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")  # type: ignore[arg-type]
    return []

def _parse_dt(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        if len(s) == 10 and s[4] == "-" and s[7] == "-":  # YYYY-MM-DD
            try:
                return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
            except Exception:
                return None
        if len(s) == 8 and s.isdigit():  # YYYYMMDD
            try:
                return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                return None
    return None

def _row_date(r: _ReportRow) -> Optional[date]:
    for k in ("report_date", "as_of_date", "as_of_dt"):
        if k in r:
            d = _parse_dt(r.get(k))
            if d:
                return d
    return None

def _filter_by_date(rows: Iterable[_ReportRow], target: Optional[date]) -> List[_ReportRow]:
    if not target:
        return list(rows)
    out: List[_ReportRow] = []
    for r in rows:
        if _row_date(r) == target:
            out.append(r)
    return out

def _call_get_dataframe(obj_cls: type, report_date: Optional[date], limit: int):
    _print_debug_object_source(obj_cls)

    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        print(f"[DQ] {obj_cls.__name__}.get_dataframe not callable; returning []")
        return [], "no_get_dataframe"

    common_kwargs = dict(limit=limit, pyspark=False)
    if _supports_kw(obj_cls, "date_formats"):
        common_kwargs["date_formats"] = ["yyyyMMdd", "yyyy-MM-dd"]

    if report_date:
        for kw in _DateKw:
            if _supports_kw(obj_cls, kw):
                try:
                    print(f"[DQ] Calling {obj_cls.__name__}.get_dataframe({kw}={report_date}, limit={limit}, pyspark=False)")
                    df = fn(**{kw: report_date, **common_kwargs})
                    recs = _to_records(df)
                    if recs:
                        return recs, f"with_kw:{kw}"
                except Exception as e:
                    print(f"[DQ] {obj_cls.__name__}.get_dataframe with {kw} failed: {e}")

    try:
        print(f"[DQ] Calling {obj_cls.__name__}.get_dataframe(limit={limit}, pyspark=False)")
        df = fn(**common_kwargs)
        recs = _to_records(df)
        if report_date:
            recs = _filter_by_date(recs, report_date)
        if recs:
            return recs, "no_kw_filtered"
    except Exception as e:
        print(f"[DQ] {obj_cls.__name__}.get_dataframe no-kw failed: {e}")

    big = max(limit, 2000)
    try:
        print(f"[DQ] Retrying {obj_cls.__name__}.get_dataframe(limit={big}, pyspark=False) for wider slice")
        common_kwargs["limit"] = big
        df = fn(**common_kwargs)
        recs = _to_records(df)
        if report_date:
            recs = _filter_by_date(recs, report_date)
        return recs, "no_kw_filtered_wide"
    except Exception as e:
        print(f"[DQ] {obj_cls.__name__}.get_dataframe wide-slice failed: {e}")
        return [], "failed_all"

class DQReports:
    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        rows, _ = _call_get_dataframe(DQSummary, report_date, limit)
        return rows

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        rows, _ = _call_get_dataframe(DQStaleness, report_date, limit)
        return rows

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        rows, _ = _call_get_dataframe(DQOutliers, report_date, limit)
        return rows

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        rows, _ = _call_get_dataframe(DQAvailability, report_date, limit)
        return rows

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        rows, _ = _call_get_dataframe(DQReasonability, report_date, limit)
        return rows

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        rows, _ = _call_get_dataframe(DQSchema, report_date, limit)
        return rows

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
                for r in rows:
                    if "report_type" not in r:
                        r["report_type"] = name
                out.extend(rows)
            else:
                print(f"[DQ] No rows for section '{name}' after all strategies.")
        return out
