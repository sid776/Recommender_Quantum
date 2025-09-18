from typing import Any, Dict, List, Optional
from datetime import date

class DQReports:
    @staticmethod
    def _as_records(df) -> List[Dict[str, Any]]:
        if df is None:
            return []
        to_dict = getattr(df, "to_dict", None)
        if callable(to_dict):
            return to_dict(orient="records")  # type: ignore[arg-type]
        return []

    @staticmethod
    def get_summary(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.summary import DQSummary
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_summary_report")
        df = DQSummary.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],  # allow both
        )
        return DQReports._as_records(df)

    @staticmethod
    def get_staleness(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.staleness import DQStaleness
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_staleness_report")
        df = DQStaleness.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],
        )
        return DQReports._as_records(df)

    @staticmethod
    def get_outliers(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.outliers import DQOutliers
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_outlier_report")
        df = DQOutliers.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],
        )
        return DQReports._as_records(df)

    @staticmethod
    def get_availability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.availability import DQAvailability
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_availability_report")
        df = DQAvailability.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],
        )
        return DQReports._as_records(df)

    @staticmethod
    def get_reasonability(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.reasonability import DQReasonability
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_reasonability_report")
        df = DQReasonability.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],
        )
        return DQReports._as_records(df)

    @staticmethod
    def get_schema(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from objects.dq.schema import DQSchema
        print("Schema and Table/View being fetched from: niwa_dev.gold.vw_smbc_marx_validation_schema_report")
        df = DQSchema.get_dataframe(
            report_date=report_date,
            limit=limit,
            pyspark=False,
            date_formats=["yyyyMMdd", "yyyy-MM-dd"],
        )
        return DQReports._as_records(df)

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
                    if "report_type" not in r:
                        r["report_type"] = name
                out.extend(rows)
        return out
