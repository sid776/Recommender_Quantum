from typing import Any, Dict, List, Optional, Union
from datetime import date, datetime
from core.db import DBConnection

VIEW_MAP = {
    "summary":       "vw_smbc_marx_validation_summary_report",
    "staleness":     "vw_smbc_marx_validation_staleness_report",
    "outliers":      "vw_smbc_marx_validation_outlier_report",
    "availability":  "vw_smbc_marx_validation_availability_report",
    "reasonability": "vw_smbc_marx_validation_reasonability_report",
    "schema":        "vw_smbc_marx_validation_schema_report",
}

CATALOG = "niwa_dev"
SCHEMA = "gold"

def _records(df) -> List[Dict[str, Any]]:
    if df is None:
        return []
    to_dict = getattr(df, "to_dict", None)
    if callable(to_dict):
        return to_dict(orient="records")
    return []

def _norm_date(val: Union[date, str]) -> str:
    if isinstance(val, (date, datetime)):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip().strip('"').strip("'")
    if len(s) == 8 and s.isdigit():  # 20250404
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s  # assume already yyyy-MM-dd

def _date_predicate(col: str, d: Union[date, str]) -> str:
    ymd_dash = _norm_date(d)
    ymd = ymd_dash.replace("-", "")
    return (
        "("
        f"to_date(CAST({col} AS STRING)) = date'{ymd_dash}' "
        f"OR to_date(regexp_replace(CAST({col} AS STRING), '-', ''), 'yyyyMMdd') = date'{ymd_dash}' "
        f"OR CAST({col} AS STRING) = '{ymd}'"
        ")"
    )

def _fetch(view_key: str, report_date: Optional[Union[date, str]], limit: int) -> List[Dict[str, Any]]:
    view = VIEW_MAP[view_key]
    fqn = f"{CATALOG}.{SCHEMA}.{view}"
    where = ""
    if report_date not in (None, "", "NULL"):
        where = f"WHERE {_date_predicate('report_date', report_date)}"
    sql = f"SELECT * FROM {fqn} {where} ORDER BY report_date DESC LIMIT {int(limit)}"
    print(f"Schema and Table/View being fetched from: {fqn}")
    with DBConnection() as db:
        df = db.execute(sql, df=True)
    rows = _records(df)
    if rows:
        for r in rows:
            r.setdefault("report_type", view_key)
    return rows

class DQReports:
    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("summary", report_date, limit)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("staleness", report_date, limit)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("outliers", report_date, limit)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("availability", report_date, limit)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("reasonability", report_date, limit)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        return _fetch("schema", report_date, limit)

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        sections = [
            ("summary",       DQReports.get_summary),
            ("staleness",     DQReports.get_staleness),
            ("outliers",      DQReports.get_outliers),
            ("availability",  DQReports.get_availability),
            ("reasonability", DQReports.get_reasonability),
            ("schema",        DQReports.get_schema),
        ]
        out: List[Dict[str, Any]] = []
        for name, getter in sections:
            rows = getter(report_date=report_date, limit=limit)
            if rows:
                for r in rows:
                    r.setdefault("report_type", name)
                out.extend(rows)
        return out
