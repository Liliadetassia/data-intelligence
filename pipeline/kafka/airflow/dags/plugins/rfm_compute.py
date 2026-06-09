"""
pipeline/airflow/plugins/rfm_compute.py
─────────────────────────────────────────
Calcula scores RFM (1-5) e segmenta usuários usando quintis.
Salva resultados em marts.rfm_segments.

Segmentos baseados no modelo RFM padrão:
  Champions        555
  Loyal            454, 545, 554
  Potential Loyal  453, 534, 535
  Promising        331, 332, 333
  Need Attention   412, 421, 422
  At Risk          241, 242, 312
  Cannot Lose Them 155, 144, 145
  Hibernating      112, 113, 121
  Lost             111
"""

import os
import logging
from datetime import datetime

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "postgres"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "user":     os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASS", "airflow"),
    "dbname":   os.getenv("DB_NAME", "ecommerce_dw"),
}

# Mapeamento de score RFM → segmento
RFM_SEGMENTS = {
    "555": "Champions",
    "554": "Champions",
    "545": "Champions",
    "544": "Loyal Customers",
    "454": "Loyal Customers",
    "445": "Loyal Customers",
    "444": "Loyal Customers",
    "453": "Potential Loyalist",
    "543": "Potential Loyalist",
    "534": "Potential Loyalist",
    "535": "Potential Loyalist",
    "344": "Potential Loyalist",
    "345": "Potential Loyalist",
    "355": "Potential Loyalist",
    "354": "Potential Loyalist",
    "552": "Recent Customers",
    "551": "Recent Customers",
    "541": "Recent Customers",
    "542": "Recent Customers",
    "533": "Promising",
    "532": "Promising",
    "523": "Promising",
    "522": "Promising",
    "512": "Promising",
    "521": "Promising",
    "511": "Promising",
    "422": "Need Attention",
    "421": "Need Attention",
    "412": "Need Attention",
    "411": "Need Attention",
    "321": "About to Sleep",
    "312": "About to Sleep",
    "311": "About to Sleep",
    "222": "About to Sleep",
    "221": "About to Sleep",
    "212": "About to Sleep",
    "211": "About to Sleep",
    "155": "Cannot Lose Them",
    "154": "Cannot Lose Them",
    "144": "Cannot Lose Them",
    "145": "Cannot Lose Them",
    "135": "Cannot Lose Them",
    "134": "Cannot Lose Them",
    "125": "Cannot Lose Them",
    "124": "Cannot Lose Them",
    "331": "At Risk",
    "332": "At Risk",
    "333": "At Risk",
    "241": "At Risk",
    "242": "At Risk",
    "243": "At Risk",
    "244": "At Risk",
    "253": "At Risk",
    "113": "Hibernating",
    "112": "Hibernating",
    "121": "Hibernating",
    "122": "Hibernating",
    "123": "Hibernating",
    "132": "Hibernating",
    "133": "Hibernating",
    "142": "Hibernating",
    "143": "Hibernating",
    "111": "Lost",
}

DEFAULT_SEGMENT = "Promising"


def _get_segment(r: int, f: int, m: int) -> str:
    key = f"{r}{f}{m}"
    if key in RFM_SEGMENTS:
        return RFM_SEGMENTS[key]
    # fallback por score médio
    avg = (r + f + m) / 3
    if avg >= 4.5:   return "Champions"
    if avg >= 3.5:   return "Loyal Customers"
    if avg >= 2.5:   return "Need Attention"
    if avg >= 1.5:   return "At Risk"
    return "Lost"


def compute_and_save_rfm() -> dict:
    """
    Lê mart_rfm_base, calcula quintis, segmenta e salva em marts.rfm_segments.
    """
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        # Lê dados do mart dbt
        df = pd.read_sql(
            "SELECT * FROM marts.mart_rfm_base",
            conn
        )
        log.info(f"RFM base: {len(df):,} usuários")

        if df.empty:
            log.warning("Nenhum dado encontrado em mart_rfm_base")
            return {"segments": {}, "total": 0}

        # ── Quintis (1=pior, 5=melhor) ──────────────
        # Recency: menor = melhor → invertido
        df["r_score"] = pd.qcut(
            df["recency_days"].clip(upper=999),
            q=5,
            labels=[5, 4, 3, 2, 1],  # invertido
            duplicates="drop"
        ).astype(int)

        # Frequency e Monetary: maior = melhor
        for col, score_col in [("frequency", "f_score"), ("monetary", "m_score")]:
            df[score_col] = pd.qcut(
                df[col],
                q=5,
                labels=[1, 2, 3, 4, 5],
                duplicates="drop"
            ).astype(int)

        # ── Score string e segmento ──────────────────
        df["rfm_score"] = (
            df["r_score"].astype(str) +
            df["f_score"].astype(str) +
            df["m_score"].astype(str)
        )

        df["segment"] = df.apply(
            lambda row: _get_segment(row["r_score"], row["f_score"], row["m_score"]),
            axis=1
        )

        # ── CLV simplificado ────────────────────────
        # CLV = avg_order_value * purchase_frequency_anual * 12 meses
        df["clv_predicted"] = (
            df["avg_order_value"].fillna(0) *
            (df["frequency"] / df["days_active"].clip(lower=1) * 365) *
            1  # 1 ano de horizonte
        ).round(2)

        # ── Churn risk proxy (será sobrescrito pelo modelo ML) ─────
        # Usuários com recency_days > 60 e f_score < 3 → alto risco
        df["churn_risk"] = np.where(
            (df["recency_days"] > 60) & (df["f_score"] < 3), 0.8,
            np.where(df["recency_days"] > 30, 0.4, 0.1)
        )

        # ── Salva no Postgres ────────────────────────
        rows = [
            (
                int(row["user_id"]),
                int(row["recency_days"]),
                int(row["frequency"]),
                float(row["monetary"]),
                int(row["r_score"]),
                int(row["f_score"]),
                int(row["m_score"]),
                row["rfm_score"],
                row["segment"],
                float(row["churn_risk"]),
                float(row["clv_predicted"]),
                datetime.utcnow(),
            )
            for _, row in df.iterrows()
        ]

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE marts.rfm_segments")
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO marts.rfm_segments (
                    user_id, recency_days, frequency, monetary,
                    r_score, f_score, m_score, rfm_score,
                    segment, churn_risk, clv_predicted, computed_at
                ) VALUES %s
                """,
                rows,
                page_size=2000,
            )
        conn.commit()

        segment_counts = df["segment"].value_counts().to_dict()
        log.info(f"✅ RFM calculado: {len(df):,} usuários | Segmentos: {segment_counts}")
        return {"total": len(df), "segment_counts": segment_counts}

    finally:
        conn.close()