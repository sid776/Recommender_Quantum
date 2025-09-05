from ninja import Router
from typing import List, Dict, Union
from core.db import DBConnection
import logging
import os

logger = logging.getLogger(__name__)
router = Router(tags=["Data APIs"])
# with DBConnection() as db:
#     print("DBConn:", type(db))
#     print("DB Driver:", os.getenv("ODBCSYSINI"), os.getenv("ODBCINI"))

CATALOG = os.getenv("DATABRICKS_CATALOG") or os.getenv("DATABRICKS_NIWA_CATALOG") or "rmdad_grc_dev"
# print("which catalog I am on")
# db.execute("SELECT current_catalog() as cat", df=True)
# print('List catalogs:')
# db.execute("SHOW CATALOGS", df=True)
# print('List schemas in the catalog:')
# db.execute("SHOW SCHEMAS in rmdad_grc_dev", df=True)
# print('List tables in schemas')
# db.execute("SHOW TABLES in rmdad_grc_dev.ussparc_silver", df=True)

@router.get("/valuation/reports/book_counts", response=List[Dict[str, Union[str, int]]])
def get_book_counts(request):
  with DBConnection() as db:

        schema = "ussparc_silver"
        table = "valuation"
        #fqt = f"[{schema}.{table}]"
        # if not cob_date:
        #     query = f"SELECT max(COB_DT) FROM rmdad_grc_dev.ussparc_silver.valuation"
        #cob_date = db.execute(query, dfa=True).values[0][0]
        cob_date = db.execute(f"SELECT max(COB_DT) FROM rmdad_grc_dev.ussparc_silver.valuation", df=True).values[0][0]
            
        query = f"""
            SELECT BOOK_NM, COUNT(*) as count
            FROM rmdad_grc_dev.ussparc_silver.valuation
            WHERE COB_DT = '{cob_date}'
            GROUP BY BOOK_NM
        """

        result = db.execute(query, df=True)
        return [{"book": row["BOOK_NM"], "count": row["count"]} for _, row in result.iterrows()]

@router.get("/riskshocks/counts", operation_id="risk_shocks_counts", response=List[Dict[str, Union[str,int]]])
def get_risk_shocks_counts(request):
    
    with DBConnection() as db:

        schema = "ussparc_silver"
        table = "riskfactor_shocks"

        #fqt = f"`{CATALOG}`.`{schema}`.`{table}`" if CATALOG else f"`{schema}`.`{table}`"
        cob_date = db.execute(f"SELECT max(SHOCK_DT) FROM rmdad_grc_dev.ussparc_silver.riskfactor_shock", df=True).values[0][0]
        query = f"""
            SELECT RF_ID, CAST(SHOCK_AM AS STRING) AS SHOCK_AM, COUNT(*) as count
            FROM rmdad_grc_dev.ussparc_silver.riskfactor_shock
            WHERE SHOCK_DT = '{cob_date}'
            GROUP BY RF_ID, SHOCK_AM
        """

        result = db.execute(query, df=True)
        return [{"risk_factor": row["RF_ID"], "curve": row["SHOCK_AM"], "count": row["count"]} for _, row in result.iterrows()]

@router.get("/sensitivities/book_counts", response=List[Dict[str, Union[str,int]]])
def get_sensitivities_counts(request):
    from core.db import DBConnection
    with DBConnection() as db:

        schema = "ussparc_silver"
        table = "sensitivity"

        cob_date = db.execute(f"SELECT max(COB_DT) FROM rmdad_grc_dev.ussparc_silver.sensitivity", df=True).values[0][0]
        query = f"""
            SELECT TRADE_NM, COUNT(*) as count
            FROM rmdad_grc_dev.{schema}.{table}
            WHERE COB_DT = '{cob_date}'
            GROUP BY TRADE_NM
        """

        result = db.execute(query, df=True)
        return [{"book": row["TRADE_NM"], "count": row["count"]} for _, row in result.iterrows()]

# @router.get("/riskshocks/counts", response=List[Dict[str, Union[str,int]]])
# def get_risk_shocks_counts(request):
#     from core.db import DBConnection
#     with DBConnection as db:

#         schema = "ussparc_silver"
#         table = "valuation"

#         cob_date = db.execute(f"SELECT max(COB_DT) FROM {schema}.{table}", dfa=True).values[0][0]
#         query = f"""
#             SELECT SCENARIO_ID, COUNT(*) as count
#             FROM {schema}.{table}
#             WHERE COB_DT = '{cob_date}'
#             GROUP BY SCENARIO_ID
#         """

#         result = db.execute(query, dfa=True)
#         return [{"scenario": row["SCENARIO_ID"], "count": row["count"]} for _, row in result.iterrows()]

@router.get("/sensitivity/pnl", response=List[Dict[str, Union[str,float]]])
def get_sensitivity_pnl(request):
    from core.db import DBConnection
    with DBConnection() as db:

        schema = "ussparc_gold"
        table = "sensitivity_pnl"

        cob_date = db.execute(f"SELECT max(COB_DT) FROM rmdad_grc_dev.ussparc_gold.sensitivity_pnl_strips", df=True).values[0][0]
        query = f"""
            SELECT BOOK_NM, SUM(PNL) as count
            FROM {schema}.{table}
            WHERE COB_DT = '{cob_date}'
            GROUP BY BOOK_NM
        """

        result = db.execute(query, df=True)
        return [{"book": row["BOOK_NM"], "pnl": row["total_pnl"], "reval_pnl": row["total_reval_pnl"]} for _, row in result.iterrows()]  

@router.get("/valuation/run_counts", response=List[Dict[str,int]])
def get_valuation_run_count(request):
    from core.db import DBConnection
    with DBConnection() as db:

        schema = "ussparc_gold"
        table = "valuation"

        cob_date = db.execute(f"SELECT max(COB_DT) FROM rmdad_grc_dev.ussparc_gold.sensitivity_pnl_strips", df=True).values[0][0]
        query = f"""
            SELECT COUNT(DISTINCT RUN_ID) as run_count
            FROM rmdad_grc_dev.ussparc_gold.sensitivity_pnl_strips
            WHERE COB_DT = '{cob_date}'
        """

        result = db.execute(query, df=True)
        return {"run_count": result.values[0][0]}
    






