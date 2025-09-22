from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ninja import Router
from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

def _norm_date_str(v: Any) -> Optional[str]:
    import pandas as pd
    if v is None:
        return None
    if isinstance(v, (pd.Timestamp, datetime, date)):
        dt = v.date() if isinstance(v, datetime) else (v if isinstance(v, date) else v.to_pydatetime().date())
        return dt.isoformat()
    s = str(v).strip().strip('"').strip("'")
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        m, d, y = s.split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        return None

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

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(
    request,
    report_date: Optional[date] = None,
    limit: int = 500,
    latest: Optional[bool] = False,
):
    rows = DQReports.get_all(report_date=report_date, limit=limit)

    if (not report_date) and latest:
        keys = ("report_date", "as_of_date", "as_of_dt", "cob_date")
        candidates: List[str] = []
        for r in rows:
            for k in keys:
                val = _norm_date_str(r.get(k))
                if val:
                    candidates.append(val)
        if candidates:
            max_d = max(candidates)
            # keep only rows that match the chosen latest date in ANY of the known fields
            filtered: List[Dict[str, Any]] = []
            for r in rows:
                if any(_norm_date_str(r.get(k)) == max_d for k in keys):
                    filtered.append(r)
            rows = filtered

    return _to_jsonable(rows)
