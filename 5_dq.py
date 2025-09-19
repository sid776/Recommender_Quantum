from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from datetime import date, datetime
import inspect
import re

_ReportRow = Dict[str, Any]
_DateKw = ("report_date", "cob_date", "as_of_date", "as_of_dt", "asof_date", "asof_dt", "cob")

_iso_like = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})?)?$")

def _supports_kw(obj_cls: type, name: str) -> bool:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return False
    try:
        sig = inspect.signature(fn)
    except Exception:
        return False
    return name in sig.parameters

def _to_records(df) -> List[_ReportRow]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")  # type: ignore[arg-type]
    return []

def _sstrip(s: str) -> str:
    return str(s).strip().strip('"').strip("'")

def _parse_dt(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        s = str(int(v))
        if len(s) == 8 and s.isdigit():
            try:
                return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                return None
        return None
    if isinstance(v, str):
        s = _sstrip(v)
        if len(s) == 8 and s.isdigit():
            try:
                return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            except Exception:
                return None
        if _iso_like.match(s):
            try:
                ss = s.replace("Z", "+00:00") if s.endswith("Z") else s
                try:
                    return datetime.fromisoformat(ss).date()
                except Exception:
                    return datetime.strptime(s[:10], "%Y-%m-%d").date()
            except Exception:
                return None
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
            except Exception:
                return None
    return None

def _candidate_date_keys(row: _ReportRow) -> List[str]:
    keys = list(row.keys())
    lcmap = {k.lower(): k for k in keys}
    preferred = [
        "report_date","reportdate","cob_date","cobdate","cob",
        "as_of_date","asofdate","as_of_dt","asof_dt","as_of","asof"
    ]
    out = [lcmap[k] for k in preferred if k in lcmap]
    for k in keys:
        lk = k.lower()
        if lk in (x for x in preferred):
            continue
        if "date" in lk or lk.endswith("_dt") or lk.endswith("_ts"):
            out.append(k)
    return out or keys

def _row_date(r: _ReportRow) -> Optional[date]:
    for k in _candidate_date_keys(r):
        d = _parse_dt(r.get(k))
        if d:
            return d
    return None

def _filter_by_date(rows: Iterable[_ReportRow], target: Optional[date]) -> List[_ReportRow]:
    if not target:
        return list(rows)
    tgt = target
    return [r for r in rows if _row_date(r) == tgt]

def _date_variants(d: date) -> List[Union[date, str, int]]:
    return [d, d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d"), int(d.strftime("%Y%m%d"))]

def _call_object_get_dataframe(obj_cls: type, report_date: Optional[date], limit: int) -> List[_ReportRow]:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return []

    def _common(order_dir: Optional[str], lim: int):
        kw = dict(limit=lim, pyspark=False)
        if _supports_kw(obj_cls, "date_formats"):
            kw["date_formats"] = ["yyyyMMdd", "yyyy-MM-dd"]
        if order_dir and _supports_kw(obj_cls, "order"):
            kw["order"] = [f"report_date {order_dir}"]
        return kw

    if report_date:
        for kwname in _DateKw:
            if _supports_kw(obj_cls, kwname):
                for variant in _date_variants(report_date):
                    try:
                        df = fn(**{kwname: variant, **_common("desc", limit)})
                        recs = _to_records(df)
                        if recs:
                            return recs
                    except Exception:
                        continue

    max_scan = 100000
    scan_sizes = [limit, 2000, 10000, 50000, max_scan]
    for order_dir in ("desc", "asc", None):
        for lim in scan_sizes:
            try:
                df = fn(**_common(order_dir, lim))
                recs = _to_records(df)
                recs = _filter_by_date(recs, report_date)
                if recs:
                    return recs
            except Exception:
                continue

    return []

def _collect_sample_rows(limit_per: int = 5000) -> List[_ReportRow]:
    from objects.dq.summary import DQSummary
    from objects.dq.staleness import DQStaleness
    from objects.dq.outliers import DQOutliers
    from objects.dq.availability import DQAvailability
    from objects.dq.reasonability import DQReasonability
    from objects.dq.schema import DQSchema
    classes = [DQSummary, DQStaleness, DQOutliers, DQAvailability, DQReasonability, DQSchema]
    out: List[_ReportRow] = []
    for cls in classes:
        fn = getattr(cls, "get_dataframe", None)
        if not callable(fn):
            continue
        kw = dict(limit=limit_per, pyspark=False)
        if _supports_kw(cls, "date_formats"):
            kw["date_formats"] = ["yyyyMMdd", "yyyy-MM-dd"]
        if _supports_kw(cls, "order"):
            kw["order"] = ["report_date desc"]
        try:
            df = fn(**kw)
            out.extend(_to_records(df))
        except Exception:
            continue
    return out

def _resolve_latest_report_date_via_objects() -> Optional[date]:
    rows = _collect_sample_rows(limit_per=5000)
    if not rows:
        return None
    dates = [d for d in (_row_date(r) for r in rows) if d is not None]
    return max(dates) if dates else None

class DQReports:
    @staticmethod
    def _ensure_report_date(report_date: Optional[date]) -> Optional[date]:
        if report_date:
            return report_date
        return _resolve_latest_report_date_via_objects()

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQSummary, rd, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQStaleness, rd, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQOutliers, rd, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQAvailability, rd, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQReasonability, rd, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        rd = DQReports._ensure_report_date(report_date)
        return _call_object_get_dataframe(DQSchema, rd, limit)

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        rd = DQReports._ensure_report_date(report_date)
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
            rows = getter(report_date=rd, limit=limit)
            if rows:
                for r in rows:
                    r.setdefault("report_type", name)
                out.extend(rows)
        return out
