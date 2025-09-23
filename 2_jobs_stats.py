# backend/services/api/jobs_stats.py
from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Callable

from ninja import Router
from django.http import JsonResponse
from util.timer import Timer

# ---------------------------------------------
# Provider discovery (no SQL in this file!)
# ---------------------------------------------
# We try to import an implementation that actually knows how to read data.
# This keeps APIs and VAReports free of SQL, matching your requirement.

_Provider = Optional[object]

def _try_import(path: str) -> _Provider:
    try:
        module_path, name = path.rsplit(".", 1)
        mod = __import__(module_path, fromlist=[name])
        return getattr(mod, name)
    except Exception:
        return None

# Order of preference for a data provider:
# 1) services.api.reports      (if you already built these helpers)
# 2) services.api.va_stats_src (alternative module name, if you use it)
# 3) objects.jobs_reports_src  (objects-backed provider)
# Each provider is expected to expose the following callables:
#   - get_latest_cob_date() -> str
#   - get_valuation_book_counts_for_date(cob_date: str, limit: int) -> List[Dict]
#   - get_risk_shocks_counts_for_date(cob_date: str, limit: int) -> List[Dict]
#   - get_sensitivities_book_counts_for_date(cob_date: str, limit: int) -> List[Dict]
#   - get_valuation_run_count_for_date(cob_date: str) -> List[Dict]
#
# If none are found, we fall back to no-op stubs returning empty results.

_provider = (
    _try_import("services.api.reports")
    or _try_import("services.api.va_stats_src")
    or _try_import("objects.jobs_reports_src")
)

def _call_or_empty(fn_name: str, *args, **kwargs):
    if _provider is not None and hasattr(_provider, fn_name):
        fn: Callable = getattr(_provider, fn_name)
        return fn(*args, **kwargs)
    # Fallbacks keep API stable even if the provider is not wired yet
    if fn_name == "get_latest_cob_date":
        return ""
    return []


# ---------------------------------------------
# VAReports facade (no SQL; delegates to provider)
# ---------------------------------------------
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


# ---------------------------------------------
# JSON normalizer (keeps outputs frontend-safe)
# ---------------------------------------------
def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        import numpy as np, math
    except Exception:
        np = None
        math = __import__("math")
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        s: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if "np" in locals() and np is not None:
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


# ---------------------------------------------
# Router (APIs exposed to frontend) – no SQL here
# ---------------------------------------------
router = Router(tags=["VA Reports"])

@router.get("/va/latest_cob")
def va_latest_cob(request):
    with Timer("Triggered va/latest_cob api") as _:
        cob = VAReports.latest_cob()
        return JsonResponse({
            "status": "success",
            "message": "Fetched latest COB date successfully...",
            "details": {"cob_date": cob},
        })


@router.get("/va/book_counts")
def va_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered va/book_counts api") as _:
        rows = _jsonable(VAReports.book_counts(cob_date=cob_date, limit=limit))
        # try to echo the effective date we used
        effective_cob = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation book counts successfully...",
            "details": rows,
            "cob_date": effective_cob,
        })


@router.get("/va/risk_shocks_counts")
def va_risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered va/risk_shocks_counts api") as _:
        rows = _jsonable(VAReports.risk_shocks_counts(cob_date=cob_date, limit=limit))
        effective_cob = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched risk shock counts successfully...",
            "details": rows,
            "cob_date": effective_cob,
        })


@router.get("/va/sensitivities_book_counts")
def va_sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered va/sensitivities_book_counts api") as _:
        rows = _jsonable(VAReports.sensitivities_book_counts(cob_date=cob_date, limit=limit))
        effective_cob = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        return JsonResponse({
            "status": "success",
            "message": "Fetched sensitivity book counts successfully...",
            "details": rows,
            "cob_date": effective_cob,
        })


@router.get("/va/valuation_run_counts")
def va_valuation_run_counts(request, cob_date: Optional[date] = None):
    with Timer("Triggered va/valuation_run_counts api") as _:
        rows = _jsonable(VAReports.valuation_run_counts(cob_date=cob_date))
        effective_cob = (cob_date.strftime("%Y-%m-%d") if cob_date else VAReports.latest_cob()) or ""
        # keep a stable shape; rows may be [{"run_count": N}] or []
        details = rows[0] if rows else {"run_count": 0}
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation run count successfully...",
            "details": details,
            "cob_date": effective_cob,
        })
