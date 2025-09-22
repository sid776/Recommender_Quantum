# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ninja import Router
from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

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
    # YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except Exception:
            return None
    # YYYYMMDD
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except Exception:
            return None
    return None

def _latest_report_date(rows: List[Dict[str, Any]]) -> Optional[date]:
    latest: Optional[date] = None
    for r in rows or []:
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
    # If no date provided, first sample rows, find the latest date, then refetch for that date.
    if not report_date:
        probe = DQReports.get_all(report_date=None, limit=limit)
        latest = _latest_report_date(probe)
        rows = DQReports.get_all(report_date=latest, limit=limit) if latest else probe
    else:
        rows = DQReports.get_all(report_date=report_date, limit=limit)

    return _to_jsonable(rows)
