# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
import inspect
import re

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

DATE_KWS = ("report_date", "as_of_date", "as_of_dt", "cob_date", "cob_dt")
# any key containing "date" or ending with "_dt" (case-insensitive) is treated as a date field
DATE_KEY_RE = re.compile(r"(date|_dt)$", re.IGNORECASE)


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
                v = v.date().isoformat()
            elif isinstance(v, date):
                v = v.isoformat()
            elif isinstance(v, Decimal):
                v = float(v)
            safe[str(k)] = v
        out.append(safe)
    return out


def _parse_date_any(v: Any) -> Optional[date]:
    """Accepts: date/datetime, 'YYYY-MM-DD', 'YYYYMMDD', ISO with time 'YYYY-MM-DDTHH:MM:SS[.fff][Z|+/-..]'."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()

    s = str(v).strip().strip("'").strip('"')
    if not s:
        return None

    # ISO with time or timezone -> take first 10 chars if they match YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except Exception:
            pass

    # YYYYMMDD
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except Exception:
            pass

    return None


def _df_to_records(df) -> List[Dict[str, Any]]:
    to_dict = getattr(df, "to_dict", None)
    return to_dict(orient="records") if callable(to_dict) else []


def _record_dates(r: Dict[str, Any]) -> List[date]:
    """Extract all parseable dates from any keys that look like dates."""
    out: List[date] = []
    for k, v in (r or {}).items():
        if DATE_KEY_RE.search(str(k)):  # key looks like a date field
            d = _parse_date_any(v)
            if d:
                out.append(d)
    # also try common names explicitly (covers e.g. 'report_date' even if regex misses somehow)
    for k in DATE_KWS:
        if k in (r or {}):
            d = _parse_date_any(r.get(k))
            if d:
                out.append(d)
    return out


def _latest_date_across_objects(limit_probe: int = 3000) -> Optional[date]:
    latest: Optional[date] = None
    for _, cls in SECTIONS:
        fn = getattr(cls, "get_dataframe", None)
        if not callable(fn):
            continue
        # wide probe (unordered) — rely on robust parsing across all likely fields
        try:
            recs = _df_to_records(fn(limit=limit_probe, pyspark=False))
            for rec in recs:
                for d in _record_dates(rec):
                    if latest is None or d > latest:
                        latest = d
        except Exception:
            pass
    return latest


def _get_for_date(cls: type, d: date, limit: int) -> List[Dict[str, Any]]:
    fn = getattr(cls, "get_dataframe", None)
    if not callable(fn):
        return []
    sig = inspect.signature(fn)
    common = dict(limit=limit, pyspark=False)

    # try calling with any supported date kwargs
    for kw in DATE_KWS:
        if kw in sig.parameters:
            try:
                recs = _df_to_records(fn(**{kw: d, **common}))
                if recs:
                    return recs
            except Exception:
                pass

    # fallback: pull and filter by any date-looking field
    try:
        recs = _df_to_records(fn(**common))
        keep: List[Dict[str, Any]] = []
        for r in recs:
            dates_in_row = _record_dates(r)
            if any(dr == d for dr in dates_in_row):
                keep.append(r)
        return keep
    except Exception:
        return []


@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    # pick the true most-recent date if not supplied
    if report_date is None:
        report_date = _latest_date_across_objects()

    rows: List[Dict[str, Any]] = []
    if report_date:
        for name, cls in SECTIONS:
            try:
                data = _get_for_date(cls, report_date, limit)
                for r in data:
                    r.setdefault("report_type", name)
                rows.extend(data)
            except Exception:
                pass

    # if still empty, as a safety net, deliver latest-available data across sections
    if not rows:
        latest = _latest_date_across_objects()
        if latest:
            for name, cls in SECTIONS:
                try:
                    data = _get_for_date(cls, latest, limit)
                    for r in data:
                        r.setdefault("report_type", name)
                    rows.extend(data)
                except Exception:
                    pass

    return _to_jsonable(rows)
