from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ninja import Router
from django.http import JsonResponse

from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

def _json_safe(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math
    import numpy as np
    import pandas as pd
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        s: Dict[str, Any] = {}
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
            s[str(k)] = v
        out.append(s)
    return out

@router.get("/dq/combined")
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    rows = DQReports.get_all(report_date=report_date, limit=limit)
    return JsonResponse({
        "status": "success",
        "message": "Fetched combined DQ reports.",
        "details": _json_safe(rows),
        "report_date": report_date.isoformat() if report_date else None,
    })

@router.get("/dq/by-type")
def dq_by_type(request, report: str, report_date: Optional[date] = None, limit: int = 500):
    rows = DQReports.get(report=report, report_date=report_date, limit=limit)
    return JsonResponse({
        "status": "success",
        "message": f"Fetched DQ report: {report}.",
        "details": _json_safe(rows),
        "report": report,
        "report_date": report_date.isoformat() if report_date else None,
    })
