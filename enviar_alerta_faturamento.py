# -*- coding: utf-8 -*-
import os
import sys
import json
import base64
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

try:
    if sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

FUSO_BRT = timezone(timedelta(hours=-3))
PROJECT_ID = "mercado-livre-mais-vendidos"
DATASET_ID = "ML_Cidades"
TABELA_ID = "Fato_MercadoLivre_MaisVendidos"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8916733671:AAH1htvd6VqDKsngdyYsFOaXdvNgUQ0RjyM").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8269176751").strip()

def obter_cliente_bigquery():
    sa_key_env = os.environ.get("GCP_SA_KEY", "").strip()
    if sa_key_env:
        try:
            if sa_key_env.startswith("{"):
                key_dict = json.loads(sa_key_env)
            else:
                key_dict = json.loads(base64.b64decode(sa_key_env).decode("utf-8"))
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return bigquery.Client(project=PROJECT_ID, credentials=creds)
        except Exception as e:
            print(f"Aviso ao carregar GCP_SA_KEY: {e}")

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
                print(f"Aviso ao ler chave local {p}: {e}")

    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"Erro ao autenticar no BigQuery: {e}")
        return None

def formatar_moeda(valor):
    if valor is None:
        return "R$ 0,00"
    v = float(valor)
    texto = f"R$ {v:,.2f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")

def consultar_faturamento(client):
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

def main():
    agora_brt = datetime.now(FUSO_BRT)
    hora_str = agora_brt.strftime("%H:%M")
    print(f"Iniciando consulta de faturamento às {hora_str} BRT...")

    client = obter_cliente_bigquery()
    if not client:
        print("Erro: Não foi possível conectar ao BigQuery.")
        sys.exit(1)

    dados = consultar_faturamento(client)
    if not dados:
        print("Erro: Consulta não retornou dados de faturamento.")
        sys.exit(1)

    fat_str = formatar_moeda(dados["faturamento_dia"])
    msg = f"Olá o Faturado até o momento {hora_str} é de {fat_str}"
    print(f"Mensagem formatada: {msg}")

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}

    for tentativa in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=25)
            if resp.status_code == 200:
                print(f"Mensagem entregue com sucesso no Telegram (Tentativa {tentativa})!")
                sys.exit(0)
            else:
                print(f"Tentativa {tentativa} falhou: HTTP {resp.status_code} - {resp.text}")
        except Exception as ex:
            print(f"Tentativa {tentativa} erro: {ex}")

    sys.exit(1)

if __name__ == "__main__":
    main()
