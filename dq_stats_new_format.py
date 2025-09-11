from datetime import date
from typing import Any, Dict, List, Optional
from ninja import Router
from core.db import DBConnection
from services.api.authentication import AzureADAuthentication

router = Router(tags=["DQ Reports"], auth=AzureADAuthentication())

def get_summary_data(report_date: Optional[date], limit: int):
    with DBConnection() as db:
        if report_date:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_summary_report
                WHERE to_date(CAST(report_date AS STRING), 'yyyy-MM-dd') = to_date('{report_date}')
                LIMIT {limit}
            """
        else:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_summary_report
                ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                LIMIT {limit}
            """
        return db.execute(q, df=True).to_dict(orient="records")

def get_staleness_data(report_date: Optional[date], limit: int):
    with DBConnection() as db:
        if report_date:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_staleness_report
                WHERE to_date(CAST(report_date AS STRING), 'yyyy-MM-dd') = to_date('{report_date}')
                LIMIT {limit}
            """
        else:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_staleness_report
                ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                LIMIT {limit}
            """
        return db.execute(q, df=True).to_dict(orient="records")

def get_outliers_data(report_date: Optional[date], limit: int):
    with DBConnection() as db:
        if report_date:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_outlier_report
                WHERE to_date(CAST(as_of_date AS STRING), 'yyyy-MM-dd') = to_date('{report_date}')
                LIMIT {limit}
            """
        else:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_outlier_report
                ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                LIMIT {limit}
            """
        return db.execute(q, df=True).to_dict(orient="records")

def get_availability_data(report_date: Optional[date], limit: int):
    with DBConnection() as db:
        if report_date:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_availability_report
                WHERE to_date(CAST(report_date AS STRING), 'yyyy-MM-dd') = to_date('{report_date}')
                LIMIT {limit}
            """
        else:
            q = f"""
                SELECT * FROM niwa_dev_gold.vw_smbc_marx_validation_availability_report
                ORDER BY to_timestamp(CAST(report_date AS STRING)) DESC
                LIMIT {limit}
            ""
