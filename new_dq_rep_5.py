from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from datetime import date, datetime
import os
import inspect

_ReportRow = Dict[str, Any]
_DateKw = ("report_date", "cob_date", "as_of_date", "as_of_dt")

FALLBACK_CATALOG = "niwa_dev"
FALLBACK_SCHEMA  = "gold"
VIEW_MAP = {
    "summary":       "vw_smbc_marx_validation_summary_report",
    "staleness":     "vw_smbc_marx_validation_staleness_report",
    "outliers":      "vw_smbc_marx_validation_outlier_report",
    "availability":  "vw_smbc_marx_validation_availability_report",
    "reasonability": "vw_smbc_marx_validation_reasonability_report",
    "schema":        "vw_smbc_marx_validation_schema_report",
}

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

def _parse_dt(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = str(v).strip().strip('"').strip("'")
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
    for k in ("report_date", "as_of_date", "as_of_dt"):
        if k in r:
            d = _parse_dt(r.get(k))
            if d:
                return d
    return None

def _filter_by_date(rows: Iterable[_ReportRow], target: Optional[date]) -> List[_ReportRow]:
    if not target:
        return list(rows)
    tgt = target
    out: List[_ReportRow] = []
    for r in rows:
        if _row_date(r) == tgt:
            out.append(r)
    return out

def _call_object_get_dataframe(obj_cls: type, report_date: Optional[date], limit: int) -> List[_ReportRow]:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return []
    common = dict(limit=limit, pyspark=False)
    if _supports_kw(obj_cls, "date_formats"):
        common["date_formats"] = ["yyyyMMdd", "yyyy-MM-dd"]
    if report_date:
        for kw in _DateKw:
            if _supports_kw(obj_cls, kw):
                try:
                    df = fn(**{kw: report_date, **common})
                    recs = _to_records(df)
                    if recs:
                        return recs
                except Exception:
                    pass
    try:
        df = fn(**common)
        recs = _to_records(df)
        return _filter_by_date(recs, report_date)
    except Exception:
        pass
    try:
        df = fn(**{**common, "limit": max(limit, 5000)})
        recs = _to_records(df)
        return _filter_by_date(recs, report_date)
    except Exception:
        return []

def _norm_date_str(d: Union[str, date, datetime]) -> str:
    if isinstance(d, (date, datetime)):
        dd = d if isinstance(d, date) and not isinstance(d, datetime) else d.date()
        return dd.strftime("%Y-%m-%d")
    s = str(d).strip().strip('"').strip("'")
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s

def _sql_latest_report_date() -> Optional[date]:
    try:
        from core.db import DBConnection
    except Exception:
        return None
    fqn = lambda v: f"{FALLBACK_CATALOG}.{FALLBACK_SCHEMA}.{v}"
    parts = []
    for v in VIEW_MAP.values():
        parts.append(
            f"SELECT MAX(to_date(CAST(report_date AS STRING))) AS d FROM {fqn(v)}"
        )
        parts.append(
            f"SELECT MAX(to_date(regexp_replace(CAST(report_date AS STRING), '-', ''), 'yyyyMMdd')) AS d FROM {fqn(v)}"
        )
    union_sql = " UNION ALL ".join(parts)
    sql = f"SELECT MAX(d) AS max_d FROM ({union_sql}) t"
    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        if df is None or df.empty:
            return None
        val = df.iloc[0, 0]
        if isinstance(val, (date, datetime)):
            return val if isinstance(val, date) and not isinstance(val, datetime) else val.date()
        parsed = _parse_dt(val)
        return parsed
    except Exception:
        return None

def _fallback_sql(view_key: str, report_date: Optional[date], limit: int) -> List[_ReportRow]:
    if os.getenv("USE_DQREPORTS_SQL_FALLBACK", "1") not in ("1", "true", "True"):
        return []
    try:
        from core.db import DBConnection
    except Exception:
        return []
    view = VIEW_MAP[view_key]
    fqn = f"{FALLBACK_CATALOG}.{FALLBACK_SCHEMA}.{view}"
    where = ""
    if report_date:
        ymd_dash = _norm_date_str(report_date)
        ymd = ymd_dash.replace("-", "")
        where = (
            " WHERE ("
            f"to_date(CAST(report_date AS STRING)) = date'{ymd_dash}' "
            f"OR to_date(regexp_replace(CAST(report_date AS STRING), '-', ''), 'yyyyMMdd') = date'{ymd_dash}' "
            f"OR CAST(report_date AS STRING) = '{ymd}'"
            ")"
        )
    sql = f"SELECT * FROM {fqn}{where} ORDER BY report_date DESC LIMIT {int(limit)}"
    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        return _to_records(df)
    except Exception:
        return []

def _fetch_section(name: str, obj_cls: type, report_date: Optional[date], limit: int) -> List[_ReportRow]:
    rows = _call_object_get_dataframe(obj_cls, report_date, limit)
    if rows:
        for r in rows:
            r.setdefault("report_type", name)
        return rows
    fb = _fallback_sql(name, report_date, limit)
    for r in fb:
        r.setdefault("report_type", name)
    return fb

class DQReports:
    @staticmethod
    def _ensure_report_date(report_date: Optional[date]) -> Optional[date]:
        if report_date:
            return report_date
        resolved = _sql_latest_report_date()
        return resolved

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("summary", DQSummary, rd, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("staleness", DQStaleness, rd, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("outliers", DQOutliers, rd, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("availability", DQAvailability, rd, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("reasonability", DQReasonability, rd, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        rd = DQReports._ensure_report_date(report_date)
        return _fetch_section("schema", DQSchema, rd, limit)

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
