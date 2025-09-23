# backend/objects/jobs_reports.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import date

# No direct SQL here — delegate to service-layer helpers
from services.api.jobs_stats import (
    get_latest_cob_date,
    get_valuation_book_counts_for_date,
    get_risk_shocks_counts_for_date,
    get_sensitivities_book_counts_for_date,
    get_valuation_run_count_for_date,
)


class VAReports:
    @staticmethod
    def _fmt_date(d: Optional[date]) -> str:
        return d.strftime("%Y-%m-%d") if d else ""

    @staticmethod
    def latest_cob() -> str:
        return get_latest_cob_date()

    @staticmethod
    def book_counts(cob_date: Optional[date], limit: int = 500) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        # returns List[{"book": str, "count": int}, ...]
        return get_valuation_book_counts_for_date(d, limit=limit)

    @staticmethod
    def risk_shocks_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        # returns List[{"risk_factor_id": int, "curve": str, "count": int}, ...]
        return get_risk_shocks_counts_for_date(d, limit=limit)

    @staticmethod
    def sensitivities_book_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        # returns List[{"book": str, "count": int}, ...]
        return get_sensitivities_book_counts_for_date(d, limit=limit)

    @staticmethod
    def valuation_run_counts(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        # returns List[{"run_count": int}]
        return get_valuation_run_count_for_date(d)
