from typing import Optional
from datetime import date
from objects.dbobject import DBModelObject, datacol

class DQSummary(DBModelObject):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_summary_report"

    @datacol(col="report_date")
    def report_date(self) -> Optional[date]:
        return None

    @datacol(col="risk_factor_id")
    def risk_factor_id(self) -> str:
        return ""

    @datacol(col="rule_type")
    def rule_type(self) -> str:
        return ""

    @datacol(col="book")
    def book(self) -> str:
        return ""

    @datacol(col="2021")
    def y2021(self) -> int:
        return 0

    @datacol(col="2022")
    def y2022(self) -> int:
        return 0

    @datacol(col="2023")
    def y2023(self) -> int:
        return 0

    @datacol(col="2024")
    def y2024(self) -> int:
        return 0

    @datacol(col="2025")
    def y2025(self) -> int:
        return 0
