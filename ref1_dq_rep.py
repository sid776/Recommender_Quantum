from typing import Any, Dict, List, Optional
from datetime import date
import pandas as pd

from core.db import DBConnection

CATALOG = "niwa_dev.gold"
VIEW_KEYS = {
    "summary":       "niwa_dev.gold.vw_smbc_marx_validation_summary_report",
    "staleness":     "niwa_dev.gold.vw_smbc_marx_validation_staleness_report",
    "outliers":      "niwa_dev.gold.vw_smbc_marx_validation_outlier_report",
    "availability":  "niwa_dev.gold.vw_smbc_marx_validation_availability_report",
    "reasonability": "niwa_dev.gold.vw_smbc_marx_validation_reasonability_report",
    "schema":        "niwa_dev.gold.vw_smbc_marx_validation_schema_report",
}

YEARS = ["2021", "2022", "2023", "2024", "2025"]
BASE_COLS = ["report_date", "risk_factor_id", "rule_type", "book", *YEARS]

class DQReports:
    @staticmethod
    def _gold(db: DBConnection) -> str:
        return db.layer_map.get("gold") or db.layer_map.get("GOLD")

    @staticmethod
    def _fqn(key: str, db: DBConnection) -> str:
        return f"{VIEW_KEYS[key]}"

    @staticmethod
    def _read(view_fqn: str, db: DBConnection) -> pd.DataFrame:
        df = db.execute(f"SELECT * FROM {{view_fqn}}", df=True)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    @staticmethod
    def _coerce_report_date(df: pd.DataFrame) -> pd.DataFrame:
        if "report_date" not in df.columns:
            df["report_date"] = pd.NaT
            return df
        dt = pd.to_datetime(df["report_date"].astype(str), errors="coerce")
        df["report_date"] = dt.dt.date
        return df

    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        for c in BASE_COLS:
            if c not in df.columns:
                df[c] = 0 if c in YEARS else ""
        for y in YEARS:
            if y in df.columns:
                df[y] = pd.to_numeric(df[y], errors="coerce").fillna(0).astype(int)
        return df

    @staticmethod
    def _latest_date(db: DBConnection) -> Optional[date]:
        for key in VIEW_KEYS:
            df = DQReports._read(DQReports._fqn(key, db), db)
            if df.empty:
                continue
            df = DQReports._coerce_report_date(df)
            if "report_date" in df.columns and not df["report_date"].isna().all():
                s = df["report_date"].dropna()
                if not s.empty:
                    return s.max()
        return None

    @staticmethod
    def get(report: str, report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        key = (report or "").lower().strip()
        if key not in VIEW_KEYS:
            return []
        with DBConnection() as db:
            d = report_date or DQReports._latest_date(db)
            df = DQReports._read(DQReports._fqn(key, db), db)
            if df.empty:
                return []
            df = DQReports._coerce_report_date(df)
            if d is not None:
                df = df[df["report_date"] == d]
            if "report_date" in df.columns:
                df = df.sort_values(by=["report_date"], ascending=False, na_position="last")
            if limit and limit > 0:
                df = df.head(int(limit))
            if df.empty:
                return []
            df = DQReports._normalize(df)
            return df.to_dict(orient="records")

    @staticmethod
    def get_all(report_date: Optional[date] = None, limit: int = 500) -> List[Dict[str, Any]]:
        frames: List[pd.DataFrame] = []
        with DBConnection() as db:
            d = report_date or DQReports._latest_date(db)
            for key in VIEW_KEYS:
                df = DQReports._read(DQReports._fqn(key, db), db)
                if df.empty:
                    continue
                df = DQReports._coerce_report_date(df)
                if d is not None:
                    df = df[df["report_date"] == d]
                if "report_date" in df.columns:
                    df = df.sort_values(by=["report_date"], ascending=False, na_position="last")
                if limit and limit > 0:
                    df = df.head(int(limit))
                if df.empty:
                    continue
                df = DQReports._normalize(df)
                df.insert(0, "report_type", key)
                frames.append(df)
        if not frames:
            return []
        out = pd.concat(frames, ignore_index=True, sort=False)
        return out.to_dict(orient="records")
