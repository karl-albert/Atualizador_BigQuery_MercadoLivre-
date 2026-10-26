#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
ALERTA EXECUTIVO ML CIDADES - APENAS O DIA EM 1 LINHA (11:00 E 15:00 BRT)
========================================================================================
Disparado diariamente às 11h00 e às 15h00 (Horário de Brasília) via GitHub Actions (ou local).
Consulta no BigQuery (ML_Cidades) e envia estritamente 1 linha com o total faturado no dia
até aquele momento para o Telegram do Karl (chat_id: 8269176751) e por E-mail (se configurado).
========================================================================================
"""

import os
import sys
import json
import time
import base64
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# Encoding UTF-8 para stdout no Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AlertaMLCidades")

FUSO_BRT = timezone(timedelta(hours=-3))

# Credenciais Oficiais
FALLBACK_TOKEN = "8916733671:AAH1htvd6VqDKsngdyYsFOaXdvNgUQ0RjyM"
FALLBACK_CHAT_ID = "8269176751"
FALLBACK_PROJECT_ID = "mercado-livre-mais-vendidos"
DATASET_ID = "ML_Cidades"
TABELA_ID = "Fato_MercadoLivre_MaisVendidos"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip() or FALLBACK_TOKEN
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip() or FALLBACK_CHAT_ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip() or os.environ.get("GCP_PROJECT", "").strip() or FALLBACK_PROJECT_ID

# Configurações de E-mail (Opcional via Secrets / Variáveis de Ambiente)
EMAIL_DESTINO = os.environ.get("EMAIL_DESTINO", "").strip()
EMAIL_REMETENTE = os.environ.get("EMAIL_REMETENTE", "").strip() or os.environ.get("SMTP_USER", "").strip()
EMAIL_SENHA = os.environ.get("EMAIL_SENHA", "").strip() or os.environ.get("SMTP_PASSWORD", "").strip()
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))


def obter_cliente_bigquery():
    """Inicializa cliente BigQuery via Secret do GitHub ou chave local."""
    sa_key_env = os.environ.get("GCP_SA_KEY", "").strip()
    
    if sa_key_env:
        try:
            logger.info("Carregando credencial GCP de GCP_SA_KEY...")
            if sa_key_env.startswith("{"):
                key_dict = json.loads(sa_key_env)
            else:
                key_dict = json.loads(base64.b64decode(sa_key_env).decode("utf-8"))
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return bigquery.Client(project=PROJECT_ID, credentials=creds)
        except Exception as e:
            logger.warning(f"Falha ao carregar credencial de GCP_SA_KEY: {e}")

    candidatos_locais = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "JSON", "mercado-livre-mais-vendidos-143cf1ecac17.json"),
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcp_key.json")
    ]

    for p in candidatos_locais:
        if os.path.exists(p):
            try:
                creds = service_account.Credentials.from_service_account_file(p)
                return bigquery.Client(project=PROJECT_ID, credentials=creds)
            except Exception as e:
                logger.warning(f"Erro ao ler chave {p}: {e}")

    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        logger.error(f"Não foi possível autenticar no BigQuery: {e}")
        return None


def formatar_moeda(valor):
    """Formata valor em Real (R$) com separadores brasileiros."""
    if valor is None:
        return "R$ 0,00"
    v = float(valor)
    texto = f"R$ {v:,.2f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def consultar_faturamento_dia(client):
    """Consulta apenas o total faturado no dia mais recente."""
    tabela_full = f"`{PROJECT_ID}.{DATASET_ID}.{TABELA_ID}`"

    query = f"""
    SELECT 
        data,
        SUM(SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64)) as faturamento_dia
    FROM {tabela_full}
    WHERE data = (SELECT MAX(data) FROM {tabela_full})
    GROUP BY data
    """
    df = client.query(query).to_dataframe()
    if len(df) > 0:
        row = df.iloc[0]
        return {
            "data": row["data"],
            "faturamento_dia": row["faturamento_dia"]
        }
    return None


def fallback_duckdb_local():
    """Fallback local para DuckDB caso BigQuery esteja inacessível."""
    import duckdb
    pq_path = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Fato_MercadoLivre_Cidades.parquet"
    if not os.path.exists(pq_path):
        return None

    con = duckdb.connect()
    q = f"""
    SELECT 
        data,
        SUM(TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE)) as faturamento_dia
    FROM '{pq_path.replace(os.sep, "/")}'
    WHERE data = (SELECT MAX(data) FROM '{pq_path.replace(os.sep, "/")}')
    GROUP BY data
    """
    df = con.execute(q).df()
    if len(df) > 0:
        row = df.iloc[0]
        return {
            "data": str(row["data"])[:10],
            "faturamento_dia": row["faturamento_dia"]
        }
    return None


def montar_mensagem_1_linha(dados, agora_brt):
    """Gera exatamente 1 única linha com a data e total faturado no dia até o momento."""
    dt_raw = dados["data"]
    if hasattr(dt_raw, "strftime"):
        dt_str = dt_raw.strftime("%d/%m")
    else:
        try:
            dt_str = datetime.strptime(str(dt_raw)[:10], "%Y-%m-%d").strftime("%d/%m")
        except:
            dt_str = str(dt_raw)

    hora_str = agora_brt.strftime("%H:%M")
    fat_str = formatar_moeda(dados["faturamento_dia"])

    # Estritamente 1 linha
    return f"🏙️ ML Cidades ({dt_str} às {hora_str}): Total faturado no dia até o momento: {fat_str}"


def enviar_telegram(texto):
    """Envia mensagem direta para o Telegram do Karl."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": texto,
        "disable_web_page_preview": True
    }
    
    for tentativa in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=25)
            if resp.status_code == 200:
                logger.info("[TELEGRAM] Posição em 1 linha entregue com sucesso!")
                return True
            else:
                logger.warning(f"[TELEGRAM] Tentativa {tentativa} HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.warning(f"[TELEGRAM] Tentativa {tentativa} erro conexão: {e}")
        time.sleep(3)

    return False


def enviar_email(texto, agora_brt):
    """Envia e-mail de 1 linha caso credenciais SMTP estejam configuradas."""
    if not (EMAIL_DESTINO and EMAIL_REMETENTE and EMAIL_SENHA):
        return False

    hora_str = agora_brt.strftime("%H:%M")
    assunto = f"ML Cidades · Faturamento do Dia às {hora_str}"

    try:
        msg = MIMEText(texto, "plain", "utf-8")
        msg["Subject"] = assunto
        msg["From"] = EMAIL_REMETENTE
        msg["To"] = EMAIL_DESTINO

        logger.info(f"[EMAIL] Enviando para {EMAIL_DESTINO}...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
            server.starttls()
            server.login(EMAIL_REMETENTE, EMAIL_SENHA)
            server.sendmail(EMAIL_REMETENTE, [EMAIL_DESTINO], msg.as_string())

        logger.info("[EMAIL] E-mail entregue com sucesso!")
        return True
    except Exception as e:
        logger.warning(f"[EMAIL] Erro no envio de e-mail: {e}")
        return False


def main():
    agora_brt = datetime.now(FUSO_BRT)
    hora_atual = agora_brt.hour

    logger.info(f"Iniciando rotina de 1 linha ML Cidades ({agora_brt.strftime('%d/%m/%Y %H:%M:%S BRT')})")

    client = obter_cliente_bigquery()
    dados = None

    if client:
        try:
            dados = consultar_faturamento_dia(client)
        except Exception as e:
            logger.error(f"Erro ao consultar BigQuery: {e}")

    if not dados:
        logger.info("Tentando DuckDB local...")
        dados = fallback_duckdb_local()

    if not dados:
        logger.error("Não foi possível obter dados de faturamento.")
        sys.exit(1)

    msg_1_linha = montar_mensagem_1_linha(dados, agora_brt)

    print("\n" + "=" * 50)
    print(msg_1_linha)
    print("=" * 50 + "\n")

    # Envia para o Telegram
    enviar_telegram(msg_1_linha)

    # Envia por E-mail (se configurado)
    enviar_email(msg_1_linha, agora_brt)


if __name__ == "__main__":
    main()
