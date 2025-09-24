# backend/services/api/jobs_stats.py
from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional, Callable
from ninja import Router
from django.http import JsonResponse
from util.timer import Timer

# -------- Provider discovery (no SQL here) --------
def _try_import(path: str):
    try:
        module_path, name = path.rsplit(".", 1)
        mod = __import__(module_path, fromlist=[name])
        return getattr(mod, name)
    except Exception:
        return None

# Prefer your concrete helpers; adjust if yours differ.
_provider = (
    _try_import("services.api.reports")
    or _try_import("services.api.jobs_stats_src")
    or _try_import("objects.jobs_reports_src")
)

def _call_or_empty(fn_name: str, *args, **kwargs):
    if _provider is not None and hasattr(_provider, fn_name):
        fn: Callable = getattr(_provider, fn_name)
        return fn(*args, **kwargs)
    if fn_name == "get_latest_cob_date":
        return ""
    return []

# -------- Facade (delegates; no SQL) --------
class VAReports:
    @staticmethod
    def _fmt_date(d: Optional[date]) -> str:
        return d.strftime("%Y-%m-%d") if d else ""

    @staticmethod
    def latest_cob() -> str:
        return _call_or_empty("get_latest_cob_date")

    @staticmethod
    def book_counts(cob_date: Optional[date], limit: int = 500) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        return _call_or_empty("get_valuation_book_counts_for_date", d, limit=limit)

    @staticmethod
    def risk_shocks_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        return _call_or_empty("get_risk_shocks_counts_for_date", d, limit=limit)

    @staticmethod
    def sensitivities_book_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        return _call_or_empty("get_sensitivities_book_counts_for_date", d, limit=limit)

    @staticmethod
    def valuation_run_counts(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        return _call_or_empty("get_valuation_run_count_for_date", d)

# -------- JSON normalizer --------
def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        import numpy as np, math
    except Exception:
        np = None
        import math  # type: ignore
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        s: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if np is not None:
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

# -------- Router (paths match Swagger) --------
router = Router(tags=["APIs exposed to frontend"])

@router.get("/valuation/reports/book_counts")
def valuation_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered valuation/reports/book_counts api"):
        rows = _jsonable(VAReports.book_counts(cob_date=cob_date, limit=limit))
        effective = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation book counts successfully...",
            "details": rows,
            "cob_date": effective,
        })

@router.get("/riskshocks/counts", operation_id="risk_shocks_counts")
def risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered riskshocks/counts api"):
        rows = _jsonable(VAReports.risk_shocks_counts(cob_date=cob_date, limit=limit))
        effective = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched risk shock counts successfully...",
            "details": rows,
            "cob_date": effective,
        })

@router.get("/sensitivities/book_counts")
def sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered sensitivities/book_counts api"):
        rows = _jsonable(VAReports.sensitivities_book_counts(cob_date=cob_date, limit=limit))
        effective = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched sensitivity book counts successfully...",
            "details": rows,
            "cob_date": effective,
        })

@router.get("/valuation/run_counts")
def valuation_run_counts(request, cob_date: Optional[date] = None):
    with Timer("Triggered valuation/run_counts api"):
        rows = _jsonable(VAReports.valuation_run_counts(cob_date=cob_date))
        effective = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        details = rows[0] if rows else {"run_count": 0}
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation run count successfully...",
            "details": details,
            "cob_date": effective,
        })
