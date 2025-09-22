# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
import inspect

from ninja import Router

# Use the objects directly per review feedback
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


def _parse_date_any(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip().strip("'").strip('"')
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
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


def _df_to_records(df) -> List[Dict[str, Any]]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")  # type: ignore[arg-type]
    return []


def _get_dataframe(obj_cls: type, report_date: Optional[date], limit: int):
    fn = getattr(obj_cls, "get_dataframe", None)
    if not callable(fn):
        return []
    common = dict(limit=limit, pyspark=False)
    sig = inspect.signature(fn)
    # try with date kw if present
    if report_date:
        for kw in DATE_KWS:
            if kw in sig.parameters:
                try:
                    df = fn(**{kw: report_date, **common})
                    recs = _df_to_records(df)
                    if recs:
                        return recs
                except Exception:
                    pass
    # fallback: no date kw, then filter in memory
    try:
        df = fn(**common)
        recs = _df_to_records(df)
        if report_date:
            want = report_date
            recs = [r for r in recs if _parse_date_any(r.get("report_date")) == want]
        return recs
    except Exception:
        return []


def _latest_date_across_objects(limit_probe: int = 1000) -> Optional[date]:
    latest: Optional[date] = None
    for _, cls in SECTIONS:
        recs = _get_dataframe(cls, None, limit_probe)
        for r in recs:
            d = _parse_date_any(r.get("report_date"))
            if d and (latest is None or d > latest):
                latest = d
    return latest


@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(
    request,
    report_date: Optional[date] = None,
    limit: int = 500,
):
    # Resolve to latest date if none supplied (so COB defaults to most recent, e.g., 2025-09-19)
    if report_date is None:
        latest = _latest_date_across_objects()
        report_date = latest

    all_rows: List[Dict[str, Any]] = []
    for name, cls in SECTIONS:
        recs = _get_dataframe(cls, report_date, limit)
        if not recs:
            continue
        for r in recs:
            r.setdefault("report_type", name)
        all_rows.extend(recs)

    return _to_jsonable(all_rows)
