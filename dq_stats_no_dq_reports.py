# backend/services/api/dq_stats.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ninja import Router

from objects.dq.summary import DQSummary
from objects.dq.staleness import DQStaleness
from objects.dq.outliers import DQOutliers
from objects.dq.availability import DQAvailability
from objects.dq.reasonability import DQReasonability
from objects.dq.schema import DQSchema

router = Router(tags=["DQ Reports"])

def _to_jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import math
    import numpy as np
    import pandas as pd
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

def _fetch_section(obj_cls: type, name: str, report_date: Optional[date], limit: int) -> List[Dict[str, Any]]:
    try:
        df = obj_cls.get_dataframe(report_date=report_date, limit=limit, pyspark=False)
        if df is not None and not df.empty:
            recs = df.to_dict(orient="records")
            for r in recs:
                r.setdefault("report_type", name)
            return recs
    except Exception:
        pass
    try:
        df = obj_cls.get_dataframe(limit=limit, pyspark=False)
        if df is not None and not df.empty:
            recs = df.to_dict(orient="records")
            for r in recs:
                r.setdefault("report_type", name)
            if report_date:
                target = str(report_date)
                recs = [x for x in recs if str(x.get("report_date", ""))[:10] == target]
            return recs
    except Exception:
        pass
    return []

@router.get("/dq/combined", response=List[Dict[str, Any]])
def dq_combined(
    request,
    report_date: Optional[date] = None,
    limit: int = 500,
):
    sections = [
        ("summary",       DQSummary),
        ("staleness",     DQStaleness),
        ("outliers",      DQOutliers),
        ("availability",  DQAvailability),
        ("reasonability", DQReasonability),
        ("schema",        DQSchema),
    ]
    rows: List[Dict[str, Any]] = []
    for name, cls in sections:
        rows.extend(_fetch_section(cls, name, report_date, limit))
    return _to_jsonable(rows)
