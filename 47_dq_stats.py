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


def _df_to_records(df) -> List[Dict[str, Any]]:
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []


def _latest_date_from_summary() -> Optional[date]:
    # Per review: use Summary object to decide the default date.
    for kwargs in (
        dict(limit=1, pyspark=False, order=['REPORT_DATE__DESC']),
        dict(limit=1, pyspark=False, order=['report_date__desc'.upper()]),
        dict(limit=500, pyspark=False),
    ):
        try:
            recs = _df_to_records(DQSummary.get_dataframe(**kwargs))
            if recs:
                d = _parse_date_any(recs[0].get("report_date")) or _parse_date_any(recs[0].get("as_of_date")) \
                    or _parse_date_any(recs[0].get("as_of_dt")) or _parse_date_any(recs[0].get("cob_date"))
                if d: return d
                # if the first row didn't have it, scan the batch
                for r in recs:
                    d2 = (_parse_date_any(r.get("report_date")) or _parse_date_any(r.get("as_of_date"))
                          or _parse_date_any(r.get("as_of_dt")) or _parse_date_any(r.get("cob_date")))
                    if d2: return d2
        except:
            pass
    return None


def _get_for_date(cls: type, d: date, limit: int) -> List[Dict[str, Any]]:
    fn = getattr(cls, "get_dataframe", None)
    if not callable(fn): return []
    sig = inspect.signature(fn)
    common = dict(limit=limit, pyspark=False)
    for kw in DATE_KWS:
        if kw in sig.parameters:
            try:
                recs = _df_to_records(fn(**{kw: d, **common}))
                if recs: return recs
            except:
                pass
    # last resort: pull and prune in-memory
    try:
        recs = _df_to_records(fn(**common))
        keep: List[Dict[str, Any]] = []
        for r in recs:
            rd = (_parse_date_any(r.get("report_date")) or _parse_date_any(r.get("as_of_date"))
                  or _parse_date_any(r.get("as_of_dt")) or _parse_date_any(r.get("cob_date")))
            if rd == d: keep.append(r)
        return keep
    except:
        return []


@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    # Resolve default date via Summary object only (no SQL, no DQReports)
    if report_date is None:
        report_date = _latest_date_from_summary()

    rows: List[Dict[str, Any]] = []
    if report_date is not None:
        for name, cls in SECTIONS:
            try:
                data = _get_for_date(cls, report_date, limit)
                for r in data: r.setdefault("report_type", name)
                rows.extend(data)
            except:
                pass
    # If still empty (e.g., no date discoverable), return the latest few from Summary so UI has something.
    if not rows:
        try:
            rows = _df_to_records(DQSummary.get_dataframe(limit=500, pyspark=False))
            for r in rows: r.setdefault("report_type", "summary")
        except:
            rows = []
    return _to_jsonable(rows)
