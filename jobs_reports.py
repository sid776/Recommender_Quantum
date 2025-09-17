# backend/objects/va_reports.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import date
import pandas as pd
import os

from core.db import DBConnection

CATALOG_SILVER = os.getenv("TABLERRICKS_SILVER_CATALOG", "ussparc_silver")
CATALOG_GOLD   = os.getenv("TABLERRICKS_GOLD_CATALOG",   "ussparc_gold")

class VAReports:
    @staticmethod
    def _fmt_date(d: Optional[date]) -> str:
        return d.strftime("%Y-%m-%d") if d else ""

    @staticmethod
    def latest_cob() -> str:
        with DBConnection() as db:
            # Prefer a stable “gold” table; fall back to silver if needed
            rows = db.execute(
                f"""
                SELECT MAX(COB_DT) AS cob FROM {CATALOG_GOLD}.sensitivity_pnl_strips
                """,
                df=True,
            )
            if rows is not None and not rows.empty and rows.iloc[0]["cob"] is not None:
                return str(rows.iloc[0]["cob"])[:10]
            rows = db.execute(
                f"SELECT MAX(COB_DT) AS cob FROM {CATALOG_SILVER}.valuation",
                df=True,
            )
            return str(rows.iloc[0]["cob"])[:10] if rows is not None and not rows.empty else ""

    @staticmethod
    def book_counts(cob_date: Optional[date], limit: int = 500) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        q = f"""
            SELECT BOOK_NM AS book, COUNT(*) AS count
            FROM {CATALOG_SILVER}.valuation
            WHERE COB_DT = to_date('{d}')
            GROUP BY BOOK_NM
            ORDER BY count DESC
            LIMIT {limit}
        """
        with DBConnection() as db:
            df: pd.DataFrame = db.execute(q, df=True)
        return df.to_dict(orient="records") if isinstance(df, pd.DataFrame) else []

    @staticmethod
    def risk_shocks_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        q = f"""
            SELECT
                RF_ID                         AS risk_factor_id,
                CAST(SHOCK_AMT AS STRING)     AS curve,
                COUNT(*)                      AS count
            FROM {CATALOG_SILVER}.riskfactor_shock
            WHERE COB_DT = to_date('{d}')
            GROUP BY RF_ID, CAST(SHOCK_AMT AS STRING)
            ORDER BY count DESC
            LIMIT {limit}
        """
        with DBConnection() as db:
            df: pd.DataFrame = db.execute(q, df=True)
        return df.to_dict(orient="records") if isinstance(df, pd.DataFrame) else []

    @staticmethod
    def sensitivities_book_counts(cob_date: Optional[date], limit: int = 2000) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        q = f"""
            SELECT BOOK_NM AS book, COUNT(*) AS count
            FROM {CATALOG_SILVER}.sensitivity
            WHERE COB_DT = to_date('{d}')
            GROUP BY BOOK_NM
            ORDER BY count DESC
            LIMIT {limit}
        """
        with DBConnection() as db:
            df: pd.DataFrame = db.execute(q, df=True)
        return df.to_dict(orient="records") if isinstance(df, pd.DataFrame) else []

    @staticmethod
    def valuation_run_counts(cob_date: Optional[date]) -> List[Dict[str, Any]]:
        d = VAReports._fmt_date(cob_date) or VAReports.latest_cob()
        q = f"""
            SELECT
                COUNT(DISTINCT RUN_ID) AS run_count
            FROM {CATALOG_GOLD}.sensitivity_pnl_strips
            WHERE COB_DT = to_date('{d}')
        """
        with DBConnection() as db:
            df: pd.DataFrame = db.execute(q, df=True)
        return df.to_dict(orient="records") if isinstance(df, pd.DataFrame) else []
