"""
pipeline/airflow/plugins/alert_engine.py
──────────────────────────────────────────
Motor de detecção de oportunidades.
Analisa segmentos RFM, churn risk e recomendações
para gerar alertas personalizados via IA (Azure OpenAI).

Tipos de alerta:
  - churn_risk:       usuários prestes a abandonar
  - reactivation:     usuários inativos com alto CLV
  - upsell:           clientes fiéis com potencial de upgrade
  - cross_sell:       oportunidade de venda cruzada
  - high_value_alert: cliente VIP com comportamento incomum
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "postgres"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "user":     os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASS", "airflow"),
    "dbname":   os.getenv("DB_NAME", "ecommerce_dw"),
}

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://models.inference.ai.azure.com")
AZURE_KEY      = os.getenv("AZURE_OPENAI_KEY", "")
AZURE_MODEL    = os.getenv("AZURE_OPENAI_MODEL", "gpt-4o-mini")

CHURN_THRESHOLD     = float(os.getenv("ALERT_CHURN_THRESHOLD", "0.7"))
MAX_ALERTS_PER_RUN  = 200


def _call_ai(prompt: str, system: str = "") -> Optional[str]:
    """Chama o Azure OpenAI / GitHub Models (gpt-4o-mini)."""
    if not AZURE_KEY:
        log.warning("AZURE_OPENAI_KEY não configurado — usando mensagem padrão")
        return None

    try:
        resp = requests.post(
            f"{AZURE_ENDPOINT}/chat/completions",
            headers={
                "Authorization": f"Bearer {AZURE_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": AZURE_MODEL,
                "messages": [
                    {"role": "system", "content": system or "Você é um assistente de e-commerce."},
                    {"role": "user",   "content": prompt},
                ],
                "max_tokens": 300,
                "temperature": 0.7,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log.error(f"Erro AI: {e}")
        return None


def _generate_message(alert_type: str, context: dict) -> str:
    """Gera mensagem personalizada via IA ou usa template padrão."""

    templates = {
        "churn_risk": (
            f"⚠️ Sentimos sua falta!\n\n"
            f"Faz {context.get('recency_days', '?')} dias desde sua última compra. "
            f"Preparamos uma oferta especial só para você:\n\n"
            f"🎁 *10% OFF* na próxima compra com o cupom: *VOLTEI10*\n\n"
            f"Válido por 48h. Aproveite! 👇"
        ),
        "reactivation": (
            f"👋 Olá! Temos novidades que você vai amar.\n\n"
            f"Com base no seu histórico, selecionamos produtos em "
            f"*{context.get('favorite_category', 'suas categorias favoritas')}* "
            f"com até *20% de desconto*.\n\n"
            f"Confira agora 👇"
        ),
        "upsell": (
            f"⭐ Você é um dos nossos clientes especiais!\n\n"
            f"Como cliente {context.get('segment', 'VIP')}, você tem acesso antecipado "
            f"aos nossos lançamentos.\n\n"
            f"Veja o que chegou de novo 👇"
        ),
        "cross_sell": (
            f"💡 Quem comprou também levou:\n\n"
            f"Clientes que compraram produtos de *{context.get('favorite_category', 'sua categoria favorita')}* "
            f"também adoraram complementar com esses itens.\n\n"
            f"Confira as sugestões personalizadas 👇"
        ),
    }

    # Tenta gerar com IA para personalização máxima
    if AZURE_KEY:
        prompt = f"""
Crie uma mensagem de WhatsApp curta e persuasiva para um cliente de e-commerce.

Tipo de alerta: {alert_type}
Dados do cliente:
- Última compra: {context.get('recency_days', '?')} dias atrás
- Total de pedidos: {context.get('frequency', '?')}
- Ticket médio: R$ {context.get('avg_order_value', '?')}
- Categoria favorita: {context.get('favorite_category', 'N/A')}
- Segmento RFM: {context.get('segment', 'N/A')}

Regras:
- Máximo 3 parágrafos curtos
- Use emojis com moderação
- Tom amigável e direto
- Inclua um call-to-action claro
- NÃO invente dados específicos de produtos
- Escreva em português brasileiro
"""
        ai_msg = _call_ai(prompt, system="Você é especialista em marketing conversacional via WhatsApp para e-commerce.")
        if ai_msg:
            return ai_msg

    return templates.get(alert_type, templates["reactivation"])


def detect_and_create_alerts() -> dict:
    """
    Detecta oportunidades e cria alertas na tabela ml.alerts.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    created = 0

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            alerts_to_insert = []

            # ── 1. Churn Risk ───────────────────────────
            cur.execute("""
                SELECT
                    r.user_id,
                    r.recency_days,
                    r.frequency,
                    r.monetary,
                    r.churn_risk,
                    r.segment,
                    r.avg_order_value,
                    m.favorite_category
                FROM marts.rfm_segments r
                LEFT JOIN marts.mart_rfm_base m ON r.user_id = m.user_id
                WHERE r.churn_risk >= %s
                  AND r.monetary > 0
                  AND NOT EXISTS (
                      SELECT 1 FROM ml.alerts a
                      WHERE a.user_id = r.user_id
                        AND a.alert_type = 'churn_risk'
                        AND a.created_at > NOW() - INTERVAL '7 days'
                  )
                ORDER BY r.monetary DESC
                LIMIT %s
            """, (CHURN_THRESHOLD, MAX_ALERTS_PER_RUN // 3))

            for row in cur.fetchall():
                ctx = dict(row)
                msg = _generate_message("churn_risk", ctx)
                alerts_to_insert.append((row["user_id"], "churn_risk", msg))

            # ── 2. Reactivation (inativos > 30d com histórico) ─────
            cur.execute("""
                SELECT
                    r.user_id,
                    r.recency_days,
                    r.frequency,
                    r.segment,
                    r.avg_order_value,
                    m.favorite_category
                FROM marts.rfm_segments r
                LEFT JOIN marts.mart_rfm_base m ON r.user_id = m.user_id
                WHERE r.recency_days BETWEEN 30 AND 90
                  AND r.frequency >= 2
                  AND r.churn_risk < %s
                  AND NOT EXISTS (
                      SELECT 1 FROM ml.alerts a
                      WHERE a.user_id = r.user_id
                        AND a.alert_type = 'reactivation'
                        AND a.created_at > NOW() - INTERVAL '14 days'
                  )
                ORDER BY r.monetary DESC
                LIMIT %s
            """, (CHURN_THRESHOLD, MAX_ALERTS_PER_RUN // 3))

            for row in cur.fetchall():
                ctx = dict(row)
                msg = _generate_message("reactivation", ctx)
                alerts_to_insert.append((row["user_id"], "reactivation", msg))

            # ── 3. Upsell para Champions/Loyal ──────────
            cur.execute("""
                SELECT
                    r.user_id,
                    r.segment,
                    r.frequency,
                    r.monetary,
                    r.avg_order_value,
                    m.favorite_category
                FROM marts.rfm_segments r
                LEFT JOIN marts.mart_rfm_base m ON r.user_id = m.user_id
                WHERE r.segment IN ('Champions', 'Loyal Customers')
                  AND NOT EXISTS (
                      SELECT 1 FROM ml.alerts a
                      WHERE a.user_id = r.user_id
                        AND a.alert_type = 'upsell'
                        AND a.created_at > NOW() - INTERVAL '30 days'
                  )
                ORDER BY r.monetary DESC
                LIMIT %s
            """, (MAX_ALERTS_PER_RUN // 3,))

            for row in cur.fetchall():
                ctx = dict(row)
                msg = _generate_message("upsell", ctx)
                alerts_to_insert.append((row["user_id"], "upsell", msg))

            # ── Insere tudo ─────────────────────────────
            if alerts_to_insert:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO ml.alerts (user_id, alert_type, message, created_at)
                    VALUES %s
                    """,
                    [(uid, atype, msg, datetime.utcnow()) for uid, atype, msg in alerts_to_insert],
                )
                created = len(alerts_to_insert)

        conn.commit()
        log.info(f"✅ {created} alertas criados")
        return {"total": created}

    finally:
        conn.close()