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
DATE_FIELDS = ("report_date", "as_of_date", "as_of_dt", "cob_date")


def _to_jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math, numpy as np, pandas as pd
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        safe: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if v is not None and hasattr(pd, "isna") and pd.isna(v): v = None
            elif isinstance(v, (np.integer,)): v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v);  v = None if math.isnan(v) or math.isinf(v) else v
            elif isinstance(v, (np.bool_,)): v = bool(v)
            elif isinstance(v, (pd.Timestamp, datetime)): v = v.date().isoformat()
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
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try: return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except: return None
    if len(s) == 8 and s.isdigit():
        try: return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except: return None
    return None


def _df_to_records(df) -> List[Dict[str, Any]]:
    return df.to_dict(orient="records") if getattr(df, "to_dict", None) else []


def _collect_all_dates(limit_probe: int = 2000) -> List[date]:
    dates: List[date] = []
    for _, cls in SECTIONS:
        fn = getattr(cls, "get_dataframe", None)
        if not callable(fn): continue
        try:
            recs = _df_to_records(fn(limit=limit_probe, pyspark=False))
            for r in recs:
                for f in DATE_FIELDS:
                    d = _parse_date_any(r.get(f))
                    if d: dates.append(d)
        except:
            pass
    return dates


def _latest_date_across_objects() -> Optional[date]:
    all_dates = _collect_all_dates()
    return max(all_dates) if all_dates else None


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
            except: pass

    try:
        recs = _df_to_records(fn(**common))
        return [r for r in recs if any(_parse_date_any(r.get(f)) == d for f in DATE_FIELDS)]
    except:
        return []


@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    if report_date is None:
        report_date = _latest_date_across_objects()

    rows: List[Dict[str, Any]] = []
    if report_date:
        for name, cls in SECTIONS:
            for r in _get_for_date(cls, report_date, limit):
                r.setdefault("report_type", name)
                rows.append(r)

    # fallback: return latest available data if still empty
    if not rows:
        latest = _latest_date_across_objects()
        if latest:
            for name, cls in SECTIONS:
                for r in _get_for_date(cls, latest, limit):
                    r.setdefault("report_type", name)
                    rows.append(r)

    return _to_jsonable(rows)
