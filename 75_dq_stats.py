# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
import inspect

from ninja import Router

from objects.dq.summary import DQSummary
from objects.dq.staleness import DQStaleness
from objects.dq.outliers import DQOutliers
from objects.dq.availability import DQAvailability
from objects.dq.reasonability import DQReasonability
from objects.dq.schema import DQSchema

router = Router(tags=["DQ Reports"])

SECTIONS = [
    ("summary",       DQSummary),
    ("staleness",     DQStaleness),
    ("outliers",      DQOutliers),
    ("availability",  DQAvailability),
    ("reasonability", DQReasonability),
    ("schema",        DQSchema),
]

# treat all of these as possible date fields
DATE_KWS   = ("report_date", "as_of_date", "as_of_dt", "cob_date")
DATE_FIELDS = DATE_KWS

# ---------- helpers ----------

def _to_jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math, numpy as np, pandas as pd
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        safe: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if v is not None and hasattr(pd, "isna") and pd.isna(v):
                v = None
            elif isinstance(v, (np.integer,)):
                v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v)
                if math.isnan(v) or math.isinf(v):
                    v = None
            elif isinstance(v, (np.bool_,)):
                v = bool(v)
            elif isinstance(v, (pd.Timestamp, datetime)):
                v = v.isoformat()
            elif isinstance(v, date):
                v = v.isoformat()
            elif isinstance(v, Decimal):
                v = float(v)
            safe[str(k)] = v
        out.append(safe)
    return out

def _norm_datestr(v: Any) -> Optional[str]:
    """Return 'YYYY-MM-DD' if v looks like a date ('YYYY-MM-DD' or 'YYYYMMDD')."""
    if v is None:
        return None
    s = str(v).strip().strip("'").strip('"')
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        # YYYY-MM-DD or similar
        try:
            return date(int(s[0:4]), int(s[5:7]), int(s[8:10])).isoformat()
        except Exception:
            return None
    if len(s) == 8 and s.isdigit():
        # YYYYMMDD
        try:
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8])).isoformat()
        except Exception:
            return None
    return None

def _parse_date_any(v: Any) -> Optional[date]:
    s = _norm_datestr(v)
    if not s:
        return None
    try:
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    except Exception:
        return None

def _df_to_records(df) -> List[Dict[str, Any]]:
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []

def _max_date_in_records(recs: List[Dict[str, Any]]) -> Optional[date]:
    """Scan rows and return the max across ANY known date field."""
    latest: Optional[date] = None
    for r in recs or []:
        for f in DATE_FIELDS:
            d = _parse_date_any(r.get(f))
            if d and (latest is None or d > latest):
                latest = d
    return latest

def _call_get_dataframe(obj_cls: type, **kwargs):
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return None
    return fn(**kwargs)

def _latest_date_across_objects() -> Optional[date]:
    """
    Try to get the most recent date across all sections using common order hints,
    then fall back to a small unordered probe.
    """
    latest: Optional[date] = None
    for _, cls in SECTIONS:
        # Fast path: try descending order hints if supported by the object
        for kwargs in (
            dict(limit=1, pyspark=False, order=['REPORT_DATE__DESC']),
            dict(limit=1, pyspark=False, order=['AS_OF_DATE__DESC']),
            dict(limit=1, pyspark=False, order=['AS_OF_DT__DESC']),
            dict(limit=1, pyspark=False, order=['COB_DATE__DESC']),
            dict(limit=1, pyspark=False, order=['report_date__desc'.upper()]),
        ):
            try:
                recs = _df_to_records(_call_get_dataframe(cls, **kwargs))
                if recs:
                    d = _max_date_in_records(recs)
                    if d and (latest is None or d > latest):
                        latest = d
                        break  # no need to try more hints for this section
            except Exception:
                pass

        # Wider unordered probe if we still don't have a date for this section
        if latest is None:
            try:
                recs = _df_to_records(_call_get_dataframe(cls, limit=300, pyspark=False))
                d = _max_date_in_records(recs)
                if d and (latest is None or d > latest):
                    latest = d
            except Exception:
                pass
    return latest

def _records_for_date(obj_cls: type, want: date, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch rows for 'want' using a date kw if the object supports one; else
    pull and filter in-memory on ANY date field.
    """
    sig = inspect.signature(getattr(obj_cls, "get_dataframe", lambda **_: None))
    common = dict(limit=limit, pyspark=False)

    # Prefer a direct call with the object's supported date kw
    for kw in DATE_KWS:
        if kw in sig.parameters:
            try:
                recs = _df_to_records(_call_get_dataframe(obj_cls, **{kw: want, **common}))
                if recs:
                    return recs
            except Exception:
                pass

    # Fallback: pull and filter on ANY recognized date column
    try:
        recs = _df_to_records(_call_get_dataframe(obj_cls, **common))
        if not recs:
            return []
        iso_want = want.isoformat()
        keep: List[Dict[str, Any]] = []
        for r in recs:
            hit = False
            for f in DATE_FIELDS:
                s = _norm_datestr(r.get(f))
                if s and s == iso_want:
                    hit = True
                    break
            if hit:
                keep.append(r)
        return keep
    except Exception:
        return []

# ---------- endpoint ----------

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    # Resolve the most recent date across objects only when not provided
    if report_date is None:
        report_date = _latest_date_across_objects()

    rows: List[Dict[str, Any]] = []
    if report_date is not None:
        for name, cls in SECTIONS:
            try:
                data = _records_for_date(cls, report_date, limit)
                for r in data:
                    r.setdefault("report_type", name)
                rows.extend(data)
            except Exception:
                pass

    # If nothing matched that date (e.g., mixed date columns), return a small recent sample so UI isn't empty
    if not rows:
        for name, cls in SECTIONS:
            try:
                data = _df_to_records(_call_get_dataframe(cls, limit=min(limit, 300), pyspark=False))
                for r in data:
                    r.setdefault("report_type", name)
                rows.extend(data)
            except Exception:
                pass

    return _to_jsonable(rows)
