from datetime import date
from typing import Any, Dict, List, Optional
import pandas as pd
from core.db import DBConnection

CATALOG = "niwa_dev.gold"

class DQSummary:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_summary_report
                    WHERE to_date(CAST(report_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_summary_report
                    ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df: pd.DataFrame = db.execute(q, df=True)
            return df.to_dict(orient="records")

class DQStaleness:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_staleness_report
                    WHERE to_date(CAST(report_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_staleness_report
                    ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df = db.execute(q, df=True)
            return df.to_dict(orient="records")

class DQOutliers:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_outlier_report
                    WHERE to_date(CAST(as_of_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_outlier_report
                    ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df = db.execute(q, df=True)
            return df.to_dict(orient="records")

class DQAvailability:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_availability_report
                    WHERE to_date(CAST(report_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_availability_report
                    ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df = db.execute(q, df=True)
            return df.to_dict(orient="records")

class DQReasonability:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_reasonability_report
                    WHERE to_date(CAST(as_of_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_reasonability_report
                    ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df = db.execute(q, df=True)
            return df.to_dict(orient="records")

class DQSchema:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_schema_report
                    WHERE to_date(CAST(as_of_date AS STRING)) = to_date('{report_date}')
                    LIMIT {limit}
                """
            else:
                q = f"""
                    SELECT * FROM {CATALOG}.vw_smbc_marx_validation_schema_report
                    ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                    LIMIT {limit}
                """
            df = db.execute(q, df=True)
            return df.to_dict(orient="records")
