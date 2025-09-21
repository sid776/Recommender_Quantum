# objects/dq_reports.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime
import os
import inspect

CATALOG = "niwa_dev.gold"

VIEW_BY_REPORT: Dict[str, str] = {
    "summary":       f"{CATALOG}.vw_smbc_marx_validation_summary_report",
    "staleness":     f"{CATALOG}.vw_smbc_marx_validation_staleness_report",
    "outliers":      f"{CATALOG}.vw_smbc_marx_validation_outlier_report",
    "availability":  f"{CATALOG}.vw_smbc_marx_validation_availability_report",
    "reasonability": f"{CATALOG}.vw_smbc_marx_validation_reasonability_report",
    "schema":        f"{CATALOG}.vw_smbc_marx_validation_schema_report",
}

USE_SQL_FALLBACK = os.getenv("USE_DQREPORTS_SQL_FALLBACK", "0").lower() in ("1", "true", "yes")
DATE_KWS = ("report_date", "cob_date", "as_of_date", "as_of_dt")

def _to_records(df) -> List[Dict[str, Any]]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")
    return []

def _parse_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
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

def _row_date(r: Dict[str, Any]) -> Optional[date]:
    for k in ("report_date", "as_of_date", "as_of_dt", "cob_date"):
        if k in r:
            return _parse_date(r.get(k))
    return None

def _filter_to_date(rows: List[Dict[str, Any]], target: Optional[date]) -> List[Dict[str, Any]]:
    if not target:
        return rows
    tgt = target
    return [r for r in rows if _row_date(r) == tgt]

def _supports_kw(obj_cls: type, kw: str) -> bool:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return False
    try:
        sig = inspect.signature(fn)
        return kw in sig.parameters
    except Exception:
        return False

def _fetch_by_object(obj_cls: type, report_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return []

    common = dict(limit=limit, pyspark=False)
    if _supports_kw(obj_cls, "order"):
        common["order"] = ["report_date__desc"]

    # 1) Try each known date kw explicitly
    if report_date:
        for kw in DATE_KWS:
            if _supports_kw(obj_cls, kw):
                try:
                    df = fn(**{kw: report_date, **common})
                    recs = _to_records(df)
                    if recs:
                        return recs
                except Exception:
                    pass

    # 2) Try unfiltered slice (ordered desc) and client-side filter to the target date
    try:
        df = fn(**common)
        recs = _to_records(df)
        if recs:
            if report_date:
                recs = _filter_to_date(recs, report_date)
            if recs:
                return recs
    except Exception:
        pass

    # 3) Try a wider slice
    wider = dict(common, limit=max(limit, 5000))
    try:
        df = fn(**wider)
        recs = _to_records(df)
        if recs and report_date:
            recs = _filter_to_date(recs, report_date)
        return recs or []
    except Exception:
        return []

def _latest_date_via_objects() -> Optional[date]:
    try:
        from objects.dq.summary import DQSummary
        rows = _fetch_by_object(DQSummary, None, limit=200)
        if not rows:
            return None
        # First row is newest if order desc applied; otherwise compute max
        d = _row_date(rows[0])
        if d:
            return d
        dates = [_row_date(r) for r in rows]
        dates = [x for x in dates if x]
        return max(dates) if dates else None
    except Exception:
        return None

def _latest_date_via_sql() -> Optional[date]:
    if not USE_SQL_FALLBACK:
        return None
    try:
        from core.db import DBConnection
    except Exception:
        return None

    norm = "COALESCE(CAST(report_date AS DATE), TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'), TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd'))"
    parts = [f"SELECT MAX({norm}) AS d FROM {v}" for v in VIEW_BY_REPORT.values()]
    sql = f"SELECT MAX(d) AS max_d FROM ({' UNION ALL '.join(parts)}) t"
    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        if df is None or df.empty:
            return None
        return _parse_date(df.iloc[0, 0])
    except Exception:
        return None

def _simple_sql_section(section: str, report_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    if not USE_SQL_FALLBACK:
        return []
    try:
        from core.db import DBConnection
    except Exception:
        return []

    view = VIEW_BY_REPORT[section]
    norm = "COALESCE(CAST(report_date AS DATE), TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'), TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd'))"
    if report_date:
        d = report_date.strftime("%Y-%m-%d")
        sql = f"SELECT * FROM {view} WHERE {norm} = DATE'{d}' ORDER BY {norm} DESC LIMIT {int(limit)}"
    else:
        sql = f"SELECT * FROM {view} ORDER BY {norm} DESC LIMIT {int(limit)}"

    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        return _to_records(df)
    except Exception:
        return []

class DQReports:
    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.summary import DQSummary
        from objects.dq.staleness import DQStaleness
        from objects.dq.outliers import DQOutliers
        from objects.dq.availability import DQAvailability
        from objects.dq.reasonability import DQReasonability
        from objects.dq.schema import DQSchema

        sections: List[Tuple[str, type]] = [
            ("summary",       DQSummary),
            ("staleness",     DQStaleness),
            ("outliers",      DQOutliers),
            ("availability",  DQAvailability),
            ("reasonability", DQReasonability),
            ("schema",        DQSchema),
        ]

        # Resolve latest date if not provided (try objects first, then SQL if enabled)
        effective_date = report_date or _latest_date_via_objects() or _latest_date_via_sql()

        out: List[Dict[str, Any]] = []

        for name, obj_cls in sections:
            rows = _fetch_by_object(obj_cls, effective_date, limit)
            if not rows and USE_SQL_FALLBACK:
                rows = _simple_sql_section(name, effective_date, limit)

            for r in rows:
                r.setdefault("report_type", name)
            out.extend(rows)

        return out
