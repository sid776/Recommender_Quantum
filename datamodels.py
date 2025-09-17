# backend/objects/dq/init.py

from .summary import DQSummary
from .staleness import DQStaleness
from .outliers import DQOutliers
from .availability import DQAvailability
from .reasonability import DQReasonability
from .schema import DQSchema

__all__ = [
    "DQSummary",
    "DQStaleness",
    "DQOutliers",
    "DQAvailability",
    "DQReasonability",
    "DQSchema",
]
##################################################################################################
#backend/objects/dq/summary.py
from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQSummary(DBModel10Object):
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
#########################################################################################
#backend/objects/dq/staleness.py

from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQStaleness(DBModel10Object):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_staleness_report"

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
###########################################################################################
#backend/objects/dq/outliers.py
from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQOutliers(DBModel10Object):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_outlier_report"

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
####################################################################################################################
#backend/objects/dq/availability.py
from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQAvailability(DBModel10Object):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_availability_report"

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
#######################################################################################################
#backend/objects/dq/reasonability.py
from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQReasonability(DBModel10Object):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_reasonability_report"

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
##################################################################################################
#backend/objects/dq/schema.py
from typing import Optional
from datetime import date
from objects.dbobject import DBModel10Object, datacol

class DQSchema(DBModel10Object):
    TABLE_SCHEMA = "gold"
    TABLE_NAME = "vw_smbc_marx_validation_schema_report"

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


