from __future__ import annotations
from datetime import date
from typing import Any, Dict, List, Optional
from ninja import Router
from services.api.authentication import AzureADAuthentication
from services.dq_services import (
    DQSummaryService,
    DQStalenessService,
    DQOutliersService,
    DQAvailabilityService,
    DQReasonabilityService,
    DQSchemaService,
)

try:
    from core.utils import Timer
except Exception:
    from contextlib import contextmanager
    @contextmanager
    def Timer(_msg: str):
        yield

router = Router(tags=["DQ Reports"], auth=AzureADAuthentication())

@router.get("/dq/summary", response=List[Dict[str, Any]])
def dq_summary(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/summary api"):
        records = DQSummaryService.get(report_date=report_date, limit=limit)
    return records

@router.get("/dq/staleness", response=List[Dict[str, Any]])
def dq_staleness(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/staleness api"):
        records = DQStalenessService.get(report_date=report_date, limit=limit)
    return records

@router.get("/dq/outliers", response=List[Dict[str, Any]])
def dq_outliers(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/outliers api"):
        records = DQOutliersService.get(report_date=report_date, limit=limit)
    return records

@router.get("/dq/availability", response=List[Dict[str, Any]])
def dq_availability(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/availability api"):
        records = DQAvailabilityService.get(report_date=report_date, limit=limit)
    return records

@router.get("/dq/reasonability", response=List[Dict[str, Any]])
def dq_reasonability(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/reasonability api"):
        records = DQReasonabilityService.get(report_date=report_date, limit=limit)
    return records

@router.get("/dq/schema", response=List[Dict[str, Any]])
def dq_schema(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/schema api"):
        records = DQSchemaService.get(report_date=report_date, limit=limit)
    return records
