from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date, datetime
import inspect

_ReportRow = Dict[str, Any]

_DATE_KWS = (
    "report_date", "report_dt",
    "cob_date", "cob_dt",
    "as_of_date", "as_of_dt",
    "cob", "date"
)

def _print_source(obj_cls: type) -> None:
    schema = getattr(obj_cls, "TABLE_SCHEMA", "?")
    table  = getattr(obj_cls, "TABLE_NAME",  "?")
    print(f"Schema and Table/View being fetched from: {schema}.{table}")

def _supports_kw(obj_cls: type, name: str) -> bool:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return False
    sig = inspect.signature(fn)
    return name in sig.parameters

def _to_records(df) -> List[_ReportRow]:
    if df is None:
        return []
    if isinstance(df, list):
        return [x for x in df if isinstance(x, dict)]
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict(orient="records")  # type: ignore[arg-type]
        except Exception:
            return []
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
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
            except Exception:
                return None
        if len(s) == 8 and s.isdigit():
            try:
                return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                return None
    return None

def _row_date(r: _ReportRow) -> Optional[date]:
    for k in ("report_date","report_dt","as_of_date","as_of_dt","cob_date","cob_dt","cob","date"):
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

def _ensure_group_keys(rows: List[_ReportRow]) -> List[_ReportRow]:
    out: List[_ReportRow] = []
    for r in rows:
        if "rule_type" not in r:
            r = {**r, "rule_type": ""}
        if "book" not in r:
            r = {**r, "book": ""}
        out.append(r)
    return out

def _date_variants(d: date) -> List[Any]:
    return [d, d.isoformat(), f"{d.year:04d}{d.month:02d}{d.day:02d}"]

def _call_get_dataframe(obj_cls: type, the_date: Optional[date], limit: int) -> List[_ReportRow]:
    _print_source(obj_cls)
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        print("get_dataframe not found")
        return []

    base_kwargs: Dict[str, Any] = {}
    if _supports_kw(obj_cls, "pyspark"):
        base_kwargs["pyspark"] = False
    max_rows = max(limit, 5000)
    if _supports_kw(obj_cls, "limit"):
        base_kwargs["limit"] = max_rows
    elif _supports_kw(obj_cls, "top"):
        base_kwargs["top"] = max_rows
    elif _supports_kw(obj_cls, "rows"):
        base_kwargs["rows"] = max_rows

    if the_date:
        for dk in _DATE_KWS:
            if _supports_kw(obj_cls, dk):
                for dv in _date_variants(the_date):
                    kwargs = {**base_kwargs, dk: dv}
                    try:
                        print(f"Calling {obj_cls.__name__}.get_dataframe with {kwargs}")
                        df = fn(**kwargs)
                        recs = _to_records(df)
                        print(f"Returned rows: {len(recs)}")
                        if recs:
                            return _ensure_group_keys(recs)
                    except Exception as e:
                        print(f"Error call {obj_cls.__name__} {kwargs}: {e}")

    try:
        print(f"Calling {obj_cls.__name__}.get_dataframe with {base_kwargs} (no date)")
        df = fn(**base_kwargs)
        recs = _to_records(df)
        print(f"Returned rows (no date): {len(recs)}")
        if the_date:
            recs = _filter_by_date(recs, the_date)
            print(f"Rows after Python-side date filter: {len(recs)}")
        if recs:
            return _ensure_group_keys(recs)
    except Exception as e:
        print(f"Error call {obj_cls.__name__} wide slice: {e}")

    return []

class DQReports:
    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        return _call_get_dataframe(DQSummary, report_date, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        return _call_get_dataframe(DQStaleness, report_date, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        return _call_get_dataframe(DQOutliers, report_date, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        return _call_get_dataframe(DQAvailability, report_date, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        return _call_get_dataframe(DQReasonability, report_date, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        return _call_get_dataframe(DQSchema, report_date, limit)

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
            print(f"Section {name}: {len(rows)} rows")
            if rows:
                for r in rows:
                    if "report_type" not in r:
                        r["report_type"] = name
                out.extend(rows)
        print(f"Combined total rows: {len(out)}")
        return out
