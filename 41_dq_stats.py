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
DATE_KWS = ("report_date", "as_of_date", "as_of_dt", "cob_date")

def _to_jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math, numpy as np, pandas as pd
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        safe: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if v is not None and hasattr(pd, "isna") and pd.isna(v):
                v = None
            elif isinstance(v, (np.integer,)): v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v)
                if math.isnan(v) or math.isinf(v): v = None
            elif isinstance(v, (np.bool_,)): v = bool(v)
            elif isinstance(v, (pd.Timestamp, datetime)): v = v.isoformat()
            elif isinstance(v, date): v = v.isoformat()
            elif isinstance(v, Decimal): v = float(v)
            safe[str(k)] = v
        out.append(safe)
    return out

def _parse_date_any(v: Any) -> Optional[date]:
    if v is None: return None
    if isinstance(v, date) and not isinstance(v, datetime): return v
    if isinstance(v, datetime): return v.date()
    s = str(v).strip().strip("'").strip('"')
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        try: return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except: return None
    if len(s) == 8 and s.isdigit():
        try: return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except: return None
    return None

def _record_date(r: Dict[str, Any]) -> Optional[date]:
    for k in DATE_KWS:
        if k in (r or {}):
            d = _parse_date_any(r.get(k))
            if d: return d
    return _parse_date_any((r or {}).get("report_date"))

def _df_to_records(df) -> List[Dict[str, Any]]:
    if df is None: return []
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []

def _call_get_dataframe(fn, sig, kwargs) -> List[Dict[str, Any]]:
    # add date_formats if accepted
    if "date_formats" in sig.parameters and "date_formats" not in kwargs:
        kwargs = {**kwargs, "date_formats": ["yyyyMMdd", "yyyy-MM-dd"]}
    return _df_to_records(fn(**kwargs))

def _get_dataframe(cls: type, report_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    fn = getattr(cls, "get_dataframe", None)
    if not callable(fn): return []
    sig = inspect.signature(fn)
    common = dict(limit=limit, pyspark=False)

    # Try with an explicit date kw if provided by the object
    if report_date:
        for kw in DATE_KWS:
            if kw in sig.parameters:
                try:
                    recs = _call_get_dataframe(fn, sig, {kw: report_date, **common})
                    if recs: return recs
                except: pass

    # Fallback: call without date; then filter in-memory to the exact date if given
    for trial_limit in (limit, max(limit, 5000)):
        try:
            recs = _call_get_dataframe(fn, sig, {**common, "limit": trial_limit})
            if not recs:
                continue
            if report_date:
                want = report_date
                recs = [r for r in recs if _record_date(r) == want]
            if recs:
                return recs
        except:  # keep going
            pass
    return []

def _latest_date_across_objects(limit_probe: int = 2000) -> Optional[date]:
    latest: Optional[date] = None
    for _, cls in SECTIONS:
        try:
            fn = getattr(cls, "get_dataframe", None)
            if not callable(fn): continue
            sig = inspect.signature(fn)
            recs = _call_get_dataframe(fn, sig, dict(limit=limit_probe, pyspark=False))
            if not recs and limit_probe < 5000:
                recs = _call_get_dataframe(fn, sig, dict(limit=5000, pyspark=False))
            for r in recs or []:
                d = _record_date(r)
                if d and (latest is None or d > latest):
                    latest = d
        except:
            pass
    return latest

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    if report_date is None:
        report_date = _latest_date_across_objects()
    all_rows: List[Dict[str, Any]] = []
    for name, cls in SECTIONS:
        recs = _get_dataframe(cls, report_date, limit)
        if not recs: continue
        for r in recs:
            r.setdefault("report_type", name)
        all_rows.extend(recs)
    return _to_jsonable(all_rows)
