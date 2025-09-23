from ninja import Router
from django.http import JsonResponse
from util.timer import Timer
from services.api.reports import (
    get_valuation_book_counts,
    get_risk_shocks_counts_for_latest_date,
    get_sensitivities_trade_counts,
    get_sensitivity_pnl_by_book,
    get_valuation_run_count_for_latest_date,
)
import logging

logger = logging.getLogger(__name__)
router = Router(tags=["APIs exposed to frontend"])


@router.get("/valuation/reports/book_counts")
def valuation_book_counts(request):
    with Timer("Triggered valuation/reports/book_counts api") as _:
        details, cob_date = get_valuation_book_counts()
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation book counts successfully...",
            "details": details,
            "cob_date": cob_date,
        })


@router.get("/riskshocks/counts", operation_id="risk_shocks_counts")
def risk_shocks_counts(request):
    with Timer("Triggered riskshocks/counts api") as _:
        details, shock_date = get_risk_shocks_counts_for_latest_date()
        return JsonResponse({
            "status": "success",
            "message": "Fetched risk shock counts successfully...",
            "details": details,
            "shock_date": shock_date,
        })


@router.get("/sensitivities/book_counts")
def sensitivities_book_counts(request):
    with Timer("Triggered sensitivities/book_counts api") as _:
        details, cob_date = get_sensitivities_trade_counts()
        return JsonResponse({
            "status": "success",
            "message": "Fetched sensitivity trade counts successfully...",
            "details": details,
            "cob_date": cob_date,
        })


@router.get("/sensitivity/pnl")
def sensitivity_pnl(request):
    with Timer("Triggered sensitivity/pnl api") as _:
        details, cob_date = get_sensitivity_pnl_by_book()
        return JsonResponse({
            "status": "success",
            "message": "Fetched sensitivity PnL successfully...",
            "details": details,
            "cob_date": cob_date,
        })


@router.get("/valuation/run_counts")
def valuation_run_counts(request):
    with Timer("Triggered valuation/run_counts api") as _:
        run_count, cob_date = get_valuation_run_count_for_latest_date()
        return JsonResponse({
            "status": "success",
            "message": "Fetched valuation run count successfully...",
            "details": {"run_count": run_count},
            "cob_date": cob_date,
        })
