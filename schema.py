#dq_stats
@router.get("/dq/schema", response_model=List[Dict[str, Any]])
def dq_schema(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/schema api"):
        records = DQSchema.get(report_date=report_date, limit=limit)
        return jsonable_encoder(records)

@router.get("/dq/schema", response=List[Dict[str, Any]])
def dq_schema(request, report_date: Optional[date] = None, limit: int = 500):
    with Timer("Triggered dq/schema api"):
        records = DQSchema.get(report_date=report_date, limit=limit)

        # Ensure it's plain JSON-safe (no NaN, no numpy.int64)
        safe_records = []
        for r in records:
            safe = {}
            for k, v in r.items():
                if isinstance(v, (np.int64, np.int32)):
                    safe[k] = int(v)
                elif isinstance(v, (np.float64, np.float32)):
                    safe[k] = float(v)
                elif pd.isna(v):
                    safe[k] = None
                else:
                    safe[k] = v
            safe_records.append(safe)

        return safe_records
###################################################################################################################
class DQSchema:
    @staticmethod
    def get(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        from core.db import DBConnection
        table = "{CATALOG}.vw_smbc_marx_validation_schema_report"

        unified_date = """
            coalesce(
                cast(report_date as date),
                to_date(cast(report_date as string), 'yyyy-MM-dd'),
                to_date(cast(report_date as string), 'yyyyMMdd')
            )
        """

        with DBConnection() as db:
            if report_date:
                rd = report_date.isoformat()  # 'YYYY-MM-DD'
                q = f"""
                    SELECT *
                    FROM {table}
                    WHERE {unified_date} = date('{rd}')
                    ORDER BY {unified_date} DESC
                    LIMIT {int(limit)}
                """
            else:
                q = f"""
                    SELECT *
                    FROM {table}
                    ORDER BY {unified_date} DESC
                    LIMIT {int(limit)}
                """

            df = db.execute(q, df=True)

        # Defensive guard
        if df is None or df.empty:
            return []

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

