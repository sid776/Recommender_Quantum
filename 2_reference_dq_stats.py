# backend/services/api/dq_stats.py
from typing import Any, Dict, List, Optional
from datetime import date
from ninja import Router

from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(
    request,
    report_date: Optional[date] = None,
    limit: int = 500,
):
    """
    Combined endpoint for all DQ reports.
    Delegates to objects.dq_reports.DQReports.get_all().
    """
    return DQReports.get_all(report_date=report_date, limit=limit)
