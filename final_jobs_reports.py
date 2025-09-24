from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional, Callable

from ninja import Router
from django.http import JsonResponse
from util.timer import Timer

def _try_import(path: str):
    try:
        module_path, name = path.rsplit(".", 1)
        mod = __import__(module_path, fromlist=[name])
        return getattr(mod, name)
    except Exception:
        return None

_provider = (
    _try_import("services.api.reports")
    or _try_import("services.api.jobs_stats_src")
    or _try_import("objects.jobs_reports_src")
)

def _call_or_empty(fn_name: str, *args, **kwargs):
    if _provider is not None and hasattr(_provider, fn_name):
        fn: Callable = getattr(_provider, fn_name)
        return fn(*args, **kwargs)
    return []

def _fmt_date(d: Optional[date]) -> str:
    return d.strftime("%Y-%m-%d") if d else ""

def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        import numpy as np, math
    except Exception:
        np = None
        import math
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

class JobsStats:
    @staticmethod
    def book_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date)
        return _call_or_empty("get_valuation_book_counts_for_date", d, limit=limit)

    @staticmethod
    def risk_shocks_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date)
        return _call_or_empty("get_risk_shocks_counts_for_date", d, limit=limit)

    @staticmethod
    def sensitivities_book_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date)
        return _call_or_empty("get_sensitivities_book_counts_for_date", d, limit=limit)

    @staticmethod
    def sensitivity_pnl_by_book(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date)
        return _call_or_empty("get_sensitivity_pnl_by_book_for_date", d) or []

router = Router(tags=["Jobs Reports"])

def _ok(message: str, details: Any, cob: str = ""):
    payload = {"status": "success", "message": message, "details": details}
    if cob:
        payload["cob_date"] = cob
    return JsonResponse(payload)

@router.get("/book_counts")
def book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("book_counts"):
        rows = _jsonable(JobsStats.book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or ""
        return _ok("Fetched book counts successfully...", rows, cob)

@router.get("/risk_shocks_counts")
def risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("risk_shocks_counts"):
        rows = _jsonable(JobsStats.risk_shocks_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or ""
        return _ok("Fetched risk shock counts successfully...", rows, cob)

@router.get("/sensitivities_book_counts")
def sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("sensitivities_book_counts"):
        rows = _jsonable(JobsStats.sensitivities_book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or ""
        return _ok("Fetched sensitivities book counts successfully...", rows, cob)

@router.get("/sensitivity_pnl")
def sensitivity_pnl(request, cob_date: Optional[date] = None):
    with Timer("sensitivity_pnl"):
        rows = _jsonable(JobsStats.sensitivity_pnl_by_book(cob_date))
        cob = _fmt_date(cob_date) or ""
        return _ok("Fetched sensitivity PnL successfully...", rows, cob)
