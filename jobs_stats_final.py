import datetime
import logging
from datetime import date
from typing import Any, Dict, List, Optional

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
        fn = getattr(_provider, fn_name)
        return fn(*args, **kwargs)
    if fn_name == "get_latest_cob_date":
        return ""
    return []

def _fmt_date(d: Optional[date]) -> str:
    return d.strftime("%Y-%m-%d") if d else ""

def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        import numpy as np, math
    except Exception:
        np = None
        import math
    out = []
    for r in rows or []:
        s = {}
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

def _latest_cob() -> str:
    return _call_or_empty("get_latest_cob_date") or ""

class VAReports:
    @staticmethod
    def book_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date) or _latest_cob()
        return _call_or_empty("get_valuation_book_counts_for_date", d, limit=limit)

    @staticmethod
    def risk_shocks_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date) or _latest_cob()
        return _call_or_empty("get_risk_shocks_counts_for_date", d, limit=limit)

    @staticmethod
    def sensitivities_book_counts(cob_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date) or _latest_cob()
        return _call_or_empty("get_sensitivities_book_counts_for_date", d, limit=limit)

    @staticmethod
    def sensitivity_pnl_by_book(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date) or _latest_cob()
        return _call_or_empty("get_sensitivity_pnl_by_book_for_date", d) or []

    @staticmethod
    def valuation_run_counts(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = _fmt_date(cob_date) or _latest_cob()
        return _call_or_empty("get_valuation_run_count_for_date", d)

router = Router(tags=["APIs exposed to frontend"])

def _ok(message: str, details: Any, cob: str = ""):
    payload = {"status": "success", "message": message, "details": details}
    if cob:
        payload["cob_date"] = cob
    return JsonResponse(payload)

@router.get("/va/latest_cob")
def va_latest_cob(request):
    with Timer("va/latest_cob"):
        cob = _latest_cob()
        return _ok("Fetched latest COB date successfully...", {"cob_date": cob})

@router.get("/va/book_counts")
def va_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("va/book_counts"):
        rows = _jsonable(VAReports.book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched valuation book counts successfully...", rows, cob)

@router.get("/valuation/reports/book_counts")
def valuation_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("valuation/reports/book_counts"):
        rows = _jsonable(VAReports.book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched valuation book counts successfully...", rows, cob)

@router.get("/va/risk_shocks_counts")
def va_risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("va/risk_shocks_counts"):
        rows = _jsonable(VAReports.risk_shocks_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched risk shock counts successfully...", rows, cob)

@router.get("/riskshocks/counts", operation_id="risk_shocks_counts")
def risk_shocks_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("riskshocks/counts"):
        rows = _jsonable(VAReports.risk_shocks_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched risk shock counts successfully...", rows, cob)

@router.get("/va/sensitivities_book_counts")
def va_sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("va/sensitivities_book_counts"):
        rows = _jsonable(VAReports.sensitivities_book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched sensitivity book counts successfully...", rows, cob)

@router.get("/sensitivities/book_counts")
def sensitivities_book_counts(request, cob_date: Optional[date] = None, limit: int = 500):
    with Timer("sensitivities/book_counts"):
        rows = _jsonable(VAReports.sensitivities_book_counts(cob_date, limit))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched sensitivity book counts successfully...", rows, cob)

@router.get("/sensitivity/pnl")
def sensitivity_pnl(request, cob_date: Optional[date] = None):
    with Timer("sensitivity/pnl"):
        rows = _jsonable(VAReports.sensitivity_pnl_by_book(cob_date))
        cob = _fmt_date(cob_date) or _latest_cob()
        return _ok("Fetched sensitivity PnL successfully...", rows, cob)

@router.get("/va/valuation_run_counts")
def va_valuation_run_counts(request, cob_date: Optional[date] = None):
    with Timer("va/valuation_run_counts"):
        rows = _jsonable(VAReports.valuation_run_counts(cob_date))
        cob = _fmt_date(cob_date) or _latest_cob()
        details = rows[0] if rows else {"run_count": 0}
        return _ok("Fetched valuation run count successfully...", details, cob)

@router.get("/valuation/run_counts")
def valuation_run_counts(request, cob_date: Optional[date] = None):
    with Timer("valuation/run_counts"):
        rows = _jsonable(VAReports.valuation_run_counts(cob_date))
        cob = _fmt_date(cob_date) or _latest_cob()
        details = rows[0] if rows else {"run_count": 0}
        return _ok("Fetched valuation run count successfully...", details, cob)
