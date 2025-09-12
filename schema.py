#dq_stats
from typing import List, Dict, Any, Optional
from datetime import date

class DQSchema:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                SELECT *
                FROM {{CATALOG}}.vw_smbc_marx_validation_schema_report
                WHERE to_date(CAST(as_of_date AS STRING)) = to_date('{report_date}')
                   OR to_date(CAST(as_of_date AS STRING), 'yyyyMMdd') = to_date('{report_date}')
                ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                LIMIT {limit}
                """
            else:
                q = f"""
                SELECT *
                FROM {{CATALOG}}.vw_smbc_marx_validation_schema_report
                ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                LIMIT {limit}
                """

            df = db.execute(q, df=True)

        # Make the frame JSON-friendly
        try:
            import numpy as np
            df = df.replace({np.nan: None})
        except Exception:
            pass

        return df.to_dict(orient="records")
#################################################################################################################################################################################
#dq_reports:
from typing import List, Dict, Any, Optional
from datetime import date

class DQSchema:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with DBConnection() as db:
            if report_date:
                q = f"""
                SELECT *
                FROM {{CATALOG}}.vw_smbc_marx_validation_schema_report
                WHERE to_date(CAST(as_of_date AS STRING)) = to_date('{report_date}')
                   OR to_date(CAST(as_of_date AS STRING), 'yyyyMMdd') = to_date('{report_date}')
                ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                LIMIT {limit}
                """
            else:
                q = f"""
                SELECT *
                FROM {{CATALOG}}.vw_smbc_marx_validation_schema_report
                ORDER BY to_timestamp(CAST(as_of_date AS STRING)) DESC
                LIMIT {limit}
                """

            df = db.execute(q, df=True)

        # Make the frame JSON-friendly
        try:
            import numpy as np
            df = df.replace({np.nan: None})
        except Exception:
            pass

        return df.to_dict(orient="records")

