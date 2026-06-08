"""
pipeline/airflow/plugins/whatsapp_sender.py
─────────────────────────────────────────────
Envia alertas pendentes via EVO API (WhatsApp).

A EVO API é compatível com o protocolo do WhatsApp Business.
Endpoint: POST /message/sendText/{instance}

Configuração:
    EVO_API_URL      ex: http://evolution.seudominio.com.br
    EVO_API_KEY      token da instância
    EVO_INSTANCE     nome da instância (ex: ecommerce-bot)
"""

import os
import time
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

EVO_API_URL  = os.getenv("EVO_API_URL", "").rstrip("/")
EVO_API_KEY  = os.getenv("EVO_API_KEY", "")
EVO_INSTANCE = os.getenv("EVO_INSTANCE", "ecommerce-bot")

DELAY_BETWEEN_MSGS = 2   # segundos entre envios (evitar ban)
MAX_RETRIES        = 2


def _send_whatsapp(phone: str, message: str) -> bool:
    """
    Envia mensagem via EVO API.

    Args:
        phone:   número no formato 5511999999999 (com DDI)
        message: texto da mensagem

    Returns:
        True se enviado com sucesso
    """
    if not EVO_API_URL or not EVO_API_KEY:
        log.warning("EVO API não configurada — simulando envio")
        log.info(f"  [SIMULADO] → {phone}: {message[:50]}...")
        return True

    url = f"{EVO_API_URL}/message/sendText/{EVO_INSTANCE}"
    payload = {
        "number":  phone,
        "text":    message,
        "delay":   1000,  # delay em ms (anti-spam do WA)
    }
    headers = {
        "apikey":       EVO_API_KEY,
        "Content-Type": "application/json",
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            if resp.status_code in (200, 201):
                return True
            else:
                log.warning(f"EVO API status {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout na tentativa {attempt + 1}")
        except Exception as e:
            log.error(f"Erro EVO API: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(3)

    return False


def _get_user_phone(cur, user_id: int) -> Optional[str]:
    """
    Busca telefone do usuário.
    NOTA: Em produção, esta tabela deve existir com os dados reais
    do seu e-commerce (Shopify/VTEX/etc).
    """
    try:
        cur.execute(
            "SELECT phone FROM staging.users WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        return row["phone"] if row else None
    except Exception:
        # Tabela pode não existir em desenvolvimento
        return None


def send_pending_alerts(limit: int = 200) -> dict:
    """
    Busca alertas não enviados e dispara via WhatsApp.

    Returns:
        dict com estatísticas do envio
    """
    conn = psycopg2.connect(**DB_CONFIG)
    stats = {"sent": 0, "failed": 0, "skipped": 0, "total": 0}

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # Busca alertas pendentes
            cur.execute("""
                SELECT
                    a.alert_id,
                    a.user_id,
                    a.alert_type,
                    a.message
                FROM ml.alerts a
                WHERE a.whatsapp_sent = FALSE
                ORDER BY
                    CASE a.alert_type
                        WHEN 'churn_risk' THEN 1
                        WHEN 'reactivation' THEN 2
                        WHEN 'upsell' THEN 3
                        ELSE 4
                    END,
                    a.created_at DESC
                LIMIT %s
            """, (limit,))

            alerts = cur.fetchall()
            stats["total"] = len(alerts)
            log.info(f"📤 Enviando {len(alerts)} alertas via WhatsApp")

            for alert in alerts:
                user_id  = alert["user_id"]
                alert_id = alert["alert_id"]
                message  = alert["message"]

                # Busca telefone
                phone = _get_user_phone(cur, user_id)

                if not phone:
                    # Em desenvolvimento: usa número placeholder para testar
                    phone = os.getenv("TEST_PHONE_NUMBER", "")
                    if not phone:
                        log.debug(f"Telefone não encontrado para user {user_id} — pulando")
                        stats["skipped"] += 1

                        # Marca como enviado em dev para não travar o pipeline
                        cur.execute("""
                            UPDATE ml.alerts
                            SET whatsapp_sent = TRUE, sent_at = NOW()
                            WHERE alert_id = %s
                        """, (alert_id,))
                        continue

                # Envia
                success = _send_whatsapp(phone, message)

                # Atualiza status
                cur.execute("""
                    UPDATE ml.alerts
                    SET whatsapp_sent = %s, sent_at = %s
                    WHERE alert_id = %s
                """, (success, datetime.utcnow() if success else None, alert_id))

                if success:
                    stats["sent"] += 1
                    log.info(f"  ✓ Alerta {alert['alert_type']} → user {user_id}")
                else:
                    stats["failed"] += 1
                    log.warning(f"  ✗ Falha → user {user_id}")

                # Flush a cada 10
                if (stats["sent"] + stats["failed"]) % 10 == 0:
                    conn.commit()

                time.sleep(DELAY_BETWEEN_MSGS)

        conn.commit()

    finally:
        conn.close()

    log.info(
        f"✅ WhatsApp concluído: {stats['sent']} enviados | "
        f"{stats['failed']} falhas | {stats['skipped']} sem telefone"
    )
    return stats