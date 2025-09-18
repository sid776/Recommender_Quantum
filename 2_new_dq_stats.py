from typing import Any, Dict, List, Optional
from datetime import date
from ninja import Router
from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    return DQReports.get_all(report_date=report_date, limit=limit)
