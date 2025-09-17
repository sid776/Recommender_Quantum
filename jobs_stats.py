# backend/services/api/va_stats.py
from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional

from ninja import Router
from services.api.authentication import AzureADAuthentication  # keep parity with your stack
from backend.objects.va_reports import VAReports

router = Router(tags=["VA Reports"])

def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import numpy as np, math
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        s: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if isinstance(v, (np.integer,)):
                v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v)
                if math.isnan(v) or math.isinf(v):
                    v = None
            elif isinstance(v, (np.bool_,)):
                v = bool(v)
            s[k] = v
        out.append(s)
    return out

@router.get("/va/latest_cob", response=Dict[str, str])
def va_latest_cob(request):
    return {"cob_date": VAReports.latest_cob()}

@router.get("/va/book_counts", response=List[Dict[str, Any]])
def va_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    return _jsonable(VAReports.book_counts(cob_date=cob_date, limit=limit))

@router.get("/va/risk_shocks_counts", response=List[Dict[str, Any]])
def va_risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 2000):
    return _jsonable(VAReports.risk_shocks_counts(cob_date=cob_date, limit=limit))

@router.get("/va/sensitivities_book_counts", response=List[Dict[str, Any]])
def va_sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 2000):
    return _jsonable(VAReports.sensitivities_book_counts(cob_date=cob_date, limit=limit))

@router.get("/va/valuation_run_counts", response=List[Dict[str, Any]])
def va_valuation_run_counts(request, cob_date: Optional[date] = None):
    return _jsonable(VAReports.valuation_run_counts(cob_date=cob_date))
