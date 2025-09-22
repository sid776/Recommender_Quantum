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
            elif isinstance(v, (np.integer,)):
                v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v);  # noqa
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

def _df_to_records(df) -> List[Dict[str, Any]]:
    if df is None: return []
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []

def _parse_dt_any(v: Any) -> Optional[date]:
    if v is None: return None
    if isinstance(v, date) and not isinstance(v, datetime): return v
    if isinstance(v, datetime): return v.date()
    s = str(v).strip().strip("'").strip('"')
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try: return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except: return None
    if len(s) == 8 and s.isdigit():
        try: return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except: return None
    return None

def _supports_kw(fn, name: str) -> bool:
    try: sig = inspect.signature(fn)
    except: return False
    return name in sig.parameters

def _latest_report_date(limit_probe: int = 2000) -> Optional[date]:
    latest: Optional[date] = None
    for _, cls in SECTIONS:
        fn = getattr(cls, "get_dataframe", None)
        if not callable(fn): continue
        try:
            df = fn(limit=limit_probe, pyspark=False)
            for r in _df_to_records(df):
                d = _parse_dt_any(r.get("report_date"))
                if d and (latest is None or d > latest):
                    latest = d
        except:
            continue
    return latest

def _get_records_for(cls: type, rd: Optional[date], limit: int) -> List[Dict[str, Any]]:
    fn = getattr(cls, "get_dataframe", None)
    if not callable(fn): return []
    common = dict(limit=limit, pyspark=False)
    if rd:
        for kw in DATE_KWS:
            if _supports_kw(fn, kw):
                try: return _df_to_records(fn(**{kw: rd, **common}))
                except: break
    try:
        recs = _df_to_records(fn(**common))
        if rd:
            recs = [r for r in recs if _parse_dt_any(r.get("report_date")) == rd]
        return recs
    except:
        return []

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    if report_date is None:
        report_date = _latest_report_date()

    rows: List[Dict[str, Any]] = []
    for name, cls in SECTIONS:
        recs = _get_records_for(cls, report_date, limit)
        for r in recs: r.setdefault("report_type", name)
        rows.extend(recs)

    return _to_jsonable(rows)
