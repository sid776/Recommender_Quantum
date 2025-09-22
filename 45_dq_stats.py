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
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try: return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except: return None
    if len(s) == 8 and s.isdigit():
        try: return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except: return None
    return None


def _df_to_records(df) -> List[Dict[str, Any]]:
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []


def _get_dataframe(cls: type, report_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    fn = getattr(cls, "get_dataframe", None)
    if not callable(fn): return []
    common = dict(limit=limit, pyspark=False)
    sig = inspect.signature(fn)

    # prefer passing a date when supported
    if report_date:
        for kw in DATE_KWS:
            if kw in sig.parameters:
                try:
                    recs = _df_to_records(fn(**{kw: report_date, **common}))
                    if recs: return recs
                except: pass

    # fallback: no date kw or empty
    try:
        recs = _df_to_records(fn(**common))
        if report_date:
            want = report_date
            recs = [r for r in recs if _parse_date_any(r.get("report_date")) == want]
        return recs
    except:
        return []


def _latest_date_from_summary() -> Optional[date]:
    # Use existing ordering convention (REPORT_DATE__DESC) seen elsewhere in your codebase
    try:
        df = DQSummary.get_dataframe(limit=1, pyspark=False, order=['REPORT_DATE__DESC'])
        recs = _df_to_records(df)
        if recs:
            return _parse_date_any(recs[0].get("report_date"))
    except:
        pass
    # very small fallback: try lowercase field name in order, then scan if needed
    try:
        df = DQSummary.get_dataframe(limit=1, pyspark=False, order=['report_date__desc'.upper()])
        recs = _df_to_records(df)
        if recs:
            return _parse_date_any(recs[0].get("report_date"))
    except:
        pass
    try:
        df = DQSummary.get_dataframe(limit=500, pyspark=False)
        recs = _df_to_records(df)
        latest: Optional[date] = None
        for r in recs:
            d = _parse_date_any(r.get("report_date"))
            if d and (latest is None or d > latest):
                latest = d
        return latest
    except:
        return None


@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    if report_date is None:
        report_date = _latest_date_from_summary()

    all_rows: List[Dict[str, Any]] = []
    for name, cls in SECTIONS:
        recs = _get_dataframe(cls, report_date, limit)
        if not recs: 
            continue
        for r in recs:
            r.setdefault("report_type", name)
        all_rows.extend(recs)

    return _to_jsonable(all_rows)
