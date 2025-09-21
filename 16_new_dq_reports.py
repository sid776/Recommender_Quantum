# objects/dq_reports.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import date
import os

# Views live under niwa_dev.gold.*
CATALOG = "niwa_dev.gold"

VIEW_BY_REPORT: Dict[str, str] = {
    "summary":       f"{CATALOG}.vw_smbc_marx_validation_summary_report",
    "staleness":     f"{CATALOG}.vw_smbc_marx_validation_staleness_report",
    "outliers":      f"{CATALOG}.vw_smbc_marx_validation_outlier_report",
    "availability":  f"{CATALOG}.vw_smbc_marx_validation_availability_report",
    "reasonability": f"{CATALOG}.vw_smbc_marx_validation_reasonability_report",
    "schema":        f"{CATALOG}.vw_smbc_marx_validation_schema_report",
}

USE_SQL_FALLBACK = os.getenv("USE_DQREPORTS_SQL_FALLBACK", "0").lower() in ("1", "true", "yes")

def _to_records(df) -> List[Dict[str, Any]]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")  # pandas
    return []

def _fetch_by_object(
    section: str,
    obj_cls: type,
    report_date: Optional[date],
    limit: int,
) -> List[Dict[str, Any]]:
    # Prefer the object model; order newest first (if supported), limit slice, materialize to pandas.
    # DBModelObject.get_dataframe supports: limit, order, pyspark
    order = ["report_date__desc"]
    try:
        df = obj_cls.get_dataframe(
            report_date=report_date,  # all your DQ* classes accept report_date
            limit=limit,
            order=order,
            pyspark=False,
        )
        rows = _to_records(df)
        for r in rows:
            r.setdefault("report_type", section)
        return rows
    except Exception:
        return []

def _sql_fallback(
    section: str,
    report_date: Optional[date],
    limit: int,
) -> List[Dict[str, Any]]:
    if not USE_SQL_FALLBACK:
        return []

    # Import DBConnection only when the fallback is actually used (to satisfy review).
    try:
        from core.db import DBConnection
    except Exception:
        return []

    view = VIEW_BY_REPORT[section]
    norm = "COALESCE(CAST(report_date AS DATE), TO_DATE(CAST(report_date AS STRING), 'yyyyMMdd'), TO_DATE(CAST(report_date AS STRING), 'yyyy-MM-dd'))"
    if report_date:
        d = report_date.strftime("%Y-%m-%d")
        sql = f"SELECT * FROM {view} WHERE {norm} = DATE'{d}' ORDER BY {norm} DESC LIMIT {int(limit)}"
    else:
        sql = f"SELECT * FROM {view} ORDER BY {norm} DESC LIMIT {int(limit)}"

    try:
        with DBConnection() as db:
            df = db.execute(sql, df=True)
        rows = _to_records(df)
        for r in rows:
            r.setdefault("report_type", section)
        return rows
    except Exception:
        return []

class DQReports:
    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        # Import the object classes here to avoid any circulars.
        from objects.dq.summary import DQSummary
        from objects.dq.staleness import DQStaleness
        from objects.dq.outliers import DQOutliers
        from objects.dq.availability import DQAvailability
        from objects.dq.reasonability import DQReasonability
        from objects.dq.schema import DQSchema

        sections: List[Tuple[str, type]] = [
            ("summary",       DQSummary),
            ("staleness",     DQStaleness),
            ("outliers",      DQOutliers),
            ("availability",  DQAvailability),
            ("reasonability", DQReasonability),
            ("schema",        DQSchema),
        ]

        out: List[Dict[str, Any]] = []

        for name, obj_cls in sections:
            # Primary: object model
            rows = _fetch_by_object(name, obj_cls, report_date, limit)
            if rows:
                out.extend(rows)
                continue

            # Optional fallback: SQL (behind a flag, default off)
            fb = _sql_fallback(name, report_date, limit)
            if fb:
                out.extend(fb)

        return out
