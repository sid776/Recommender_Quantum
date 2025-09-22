# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ninja import Router
from core.db import DBConnection
from objects.dq_reports import DQReports

router = Router(tags=["DQ Reports"])

CATALOG = "niwa_dev.gold"
VIEWS = [
    f"{CATALOG}.vw_smbc_marx_validation_summary_report",
    f"{CATALOG}.vw_smbc_marx_validation_staleness_report",
    f"{CATALOG}.vw_smbc_marx_validation_outlier_report",
    f"{CATALOG}.vw_smbc_marx_validation_availability_report",
    f"{CATALOG}.vw_smbc_marx_validation_reasonability_report",
    f"{CATALOG}.vw_smbc_marx_validation_schema_report",
]

def _to_jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math, numpy as np, pandas as pd
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        safe: Dict[str, Any] = {}
        for k, v in (r or {}).items():
            if v is not None and hasattr(pd, "isna") and pd.isna(v):
                v = None
            elif isinstance(v, (np.integer,)):
                v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v)
                if math.isnan(v) or math.isinf(v):
                    v = None
            elif isinstance(v, (np.bool_,)):
                v = bool(v)
            elif isinstance(v, (pd.Timestamp, datetime)):
                v = v.isoformat()
            elif isinstance(v, date):
                v = v.isoformat()
            elif isinstance(v, Decimal):
                v = float(v)
            safe[str(k)] = v
        out.append(safe)
    return out

def _latest_report_date_sql() -> Optional[date]:
    norm = "COALESCE(CAST(report_date AS DATE), TO_DATE(CAST(report_date AS STRING),'yyyyMMdd'), TO_DATE(CAST(report_date AS STRING),'yyyy-MM-dd'))"
    unions = " UNION ALL ".join([f"SELECT MAX({norm}) d FROM {v}" for v in VIEWS])
    sql = f"SELECT MAX(d) AS max_d FROM ({unions}) t"
    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        if df is None or df.empty:
            return None
        val = df.iloc[0, 0]
        if isinstance(val, datetime):
            return val.date()
        return val
    except:
        return None

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(request, report_date: Optional[date] = None, limit: int = 500):
    if report_date is None:
        report_date = _latest_report_date_sql()
    rows = DQReports.get_all(report_date=report_date, limit=limit)
    return _to_jsonable(rows)
