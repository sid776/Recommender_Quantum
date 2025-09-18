from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime

_ReportRow = Dict[str, Any]

class DQReports:
    @staticmethod
    def _as_records(df) -> List[_ReportRow]:
        if df is None:
            return []
        to_dict = getattr(df, "to_dict", None)
        if callable(to_dict):
            return to_dict(orient="records")  # type: ignore[arg-type]
        return []

    @staticmethod
    def _parse_dt(v: Any) -> Optional[date]:
        if v is None:
            return None
        if isinstance(v, date) and not isinstance(v, datetime):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            s = v.strip()
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                # YYYY-MM-DD
                try:
                    return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
                except Exception:
                    return None
            if len(s) == 8 and s.isdigit():
                # YYYYMMDD
                try:
                    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                except Exception:
                    return None
        return None

    @staticmethod
    def _row_date(r: _ReportRow) -> Optional[date]:
        # Try common keys the lake uses across views
        for k in ("report_date", "as_of_date", "as_of_dt"):
            if k in r:
                d = DQReports._parse_dt(r.get(k))
                if d:
                    return d
        return None

    @staticmethod
    def _filter_by_date(rows: Iterable[_ReportRow], target: Optional[date]) -> List[_ReportRow]:
        if not target:
            return list(rows)
        out: List[_ReportRow] = []
        for r in rows:
            rd = DQReports._row_date(r)
            if rd == target:
                out.append(r)
        return out

    @staticmethod
    def _fetch_records(ObjCls, limit: int) -> List[_ReportRow]:
        # Don’t assume arg names like report_date/cob_date; ask for recent rows only
        df = ObjCls.get_dataframe(limit=limit, pyspark=False)  # type: ignore[attr-defined]
        return DQReports._as_records(df)

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.summary import DQSummary
        rows = DQReports._fetch_records(DQSummary, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.staleness import DQStaleness
        rows = DQReports._fetch_records(DQStaleness, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.outliers import DQOutliers
        rows = DQReports._fetch_records(DQOutliers, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.availability import DQAvailability
        rows = DQReports._fetch_records(DQAvailability, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.reasonability import DQReasonability
        rows = DQReports._fetch_records(DQReasonability, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        from objects.dq.schema import DQSchema
        rows = DQReports._fetch_records(DQSchema, limit)
        return DQReports._filter_by_date(rows, report_date)

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[_ReportRow]:
        sections = [
            ("summary",       DQReports.get_summary),
            ("staleness",     DQReports.get_staleness),
            ("outliers",      DQReports.get_outliers),
            ("availability",  DQReports.get_availability),
            ("reasonability", DQReports.get_reasonability),
            ("schema",        DQReports.get_schema),
        ]
        out: List[_ReportRow] = []
        for name, getter in sections:
            rows = getter(report_date=report_date, limit=limit)
            if rows:
                for r in rows:
                    if "report_type" not in r:
                        r["report_type"] = name
                out.extend(rows)
        return out
