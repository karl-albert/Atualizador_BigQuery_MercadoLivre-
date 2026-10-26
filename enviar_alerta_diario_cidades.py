#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
ALERTA EXECUTIVO DIÁRIO - ML CIDADES (TELEGRAM BOT DAS 11:00 BRT)
========================================================================================
Disparado diariamente às 11h00 (Horário de Brasília) via GitHub Actions (ou local).
Consulta os números consolidados de faturamento no BigQuery (ML_Cidades) e envia
o boletim executivo com o faturamento acumulado e indicadores chave para o Telegram.
========================================================================================
"""

import os
import sys
import json
import time
import base64
import logging
from datetime import datetime
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AlertaMLCidades")

# Constantes e Credenciais
FALLBACK_TOKEN = "8916733671:AAH1htvd6VqDKsngdyYsFOaXdvNgUQ0RjyM"
FALLBACK_CHAT_ID = "891673367"
FALLBACK_PROJECT_ID = "mercado-livre-mais-vendidos"
DATASET_ID = "ML_Cidades"
TABELA_ID = "Fato_MercadoLivre_MaisVendidos"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip() or FALLBACK_TOKEN
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip() or FALLBACK_CHAT_ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip() or os.environ.get("GCP_PROJECT", "").strip() or FALLBACK_PROJECT_ID


def obter_cliente_bigquery():
    """Inicializa o cliente do BigQuery via secret do GitHub ou arquivo local."""
    sa_key_env = os.environ.get("GCP_SA_KEY", "").strip()
    
    if sa_key_env:
        try:
            logger.info("Carregando credencial GCP da variável de ambiente...")
            if sa_key_env.startswith("{"):
                key_dict = json.loads(sa_key_env)
            else:
                key_dict = json.loads(base64.b64decode(sa_key_env).decode("utf-8"))
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return bigquery.Client(project=PROJECT_ID, credentials=creds)
        except Exception as e:
            logger.warning(f"Falha ao carregar credencial de GCP_SA_KEY: {e}")

    # Fallbacks locais no Windows
    candidatos_locais = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "JSON", "mercado-livre-mais-vendidos-143cf1ecac17.json"),
        r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcp_key.json")
    ]

    for p in candidatos_locais:
        if os.path.exists(p):
            try:
                logger.info(f"Carregando credencial GCP do arquivo local: {p}")
                creds = service_account.Credentials.from_service_account_file(p)
                return bigquery.Client(project=PROJECT_ID, credentials=creds)
            except Exception as e:
                logger.warning(f"Erro ao ler chave {p}: {e}")

    try:
        logger.info("Tentando autenticação padrão do ambiente Google Cloud...")
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


def formatar_milhoes_bi(valor):
    """Formata valor resumido em Milhões ou Bilhões."""
    if valor is None:
        return "R$ 0"
    v = float(valor)
    if abs(v) >= 1_000_000_000:
        return f"R$ {v / 1_000_000_000:.2f} Bi"
    elif abs(v) >= 1_000_000:
        return f"R$ {v / 1_000_000:.2f} Mi"
    elif abs(v) >= 1_000:
        return f"R$ {v / 1_000:.2f} Mil"
    return f"R$ {v:.2f}"


def consultar_metricas_cidades(client):
    """Executa a query agregada no BigQuery na tabela de Cidades."""
    tabela_full = f"`{PROJECT_ID}.{DATASET_ID}.{TABELA_ID}`"
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month

    logger.info(f"Consultando métricas agregadas na tabela {tabela_full}...")

    query_totais = f"""
    SELECT 
        MAX(data) as max_data,
        COUNT(*) as total_linhas,
        SUM(SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64)) as faturamento_total_historico,
        SUM(CASE WHEN EXTRACT(YEAR FROM data) = {ano_atual} THEN SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64) ELSE 0 END) as faturamento_ytd,
        SUM(CASE WHEN EXTRACT(YEAR FROM data) = {ano_atual} AND EXTRACT(MONTH FROM data) = {mes_atual} THEN SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64) ELSE 0 END) as faturamento_mtd
    FROM {tabela_full}
    """
    df_totais = client.query(query_totais).to_dataframe()
    totais = df_totais.iloc[0].to_dict()

    # Último dia fechado
    max_data = totais["max_data"]
    query_ultimo_dia = f"""
    SELECT 
        data,
        COUNT(*) as linhas_dia,
        SUM(SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64)) as fat_dia,
        SUM(qtd_vendas_estimadas_dia) as vendas_dia
    FROM {tabela_full}
    WHERE data = '{max_data}'
    GROUP BY data
    """
    df_dia = client.query(query_ultimo_dia).to_dataframe()
    ultimo_dia = df_dia.iloc[0].to_dict() if len(df_dia) > 0 else {}

    # Top 5 UFs do ano
    query_top_ufs = f"""
    SELECT 
        uf,
        SUM(SAFE_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS FLOAT64)) as fat_uf
    FROM {tabela_full}
    WHERE EXTRACT(YEAR FROM data) = {ano_atual}
    GROUP BY uf
    ORDER BY fat_uf DESC
    LIMIT 5
    """
    df_ufs = client.query(query_top_ufs).to_dataframe()
    top_ufs = df_ufs.to_dict(orient="records")

    return {
        "max_data": max_data,
        "ano_atual": ano_atual,
        "mes_atual": mes_atual,
        "totais": totais,
        "ultimo_dia": ultimo_dia,
        "top_ufs": top_ufs
    }


def fallback_duckdb_local():
    """Consulta os dados locais via DuckDB caso o BigQuery esteja offline."""
    import duckdb
    pq_path = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Fato_MercadoLivre_Cidades.parquet"
    if not os.path.exists(pq_path):
        return None

    logger.info(f"Executando consulta via DuckDB local em: {pq_path}")
    con = duckdb.connect()
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month

    q = f"""
    SELECT 
        MAX(data) as max_data,
        COUNT(*) as total_linhas,
        SUM(TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE)) as faturamento_total_historico,
        SUM(CASE WHEN ano = {ano_atual} THEN TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE) ELSE 0 END) as faturamento_ytd,
        SUM(CASE WHEN ano = {ano_atual} AND mes = {mes_atual} THEN TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE) ELSE 0 END) as faturamento_mtd
    FROM '{pq_path.replace(os.sep, "/")}'
    """
    df_totais = con.execute(q).df()
    totais = df_totais.iloc[0].to_dict()
    max_data = str(totais["max_data"])[:10]

    q_dia = f"""
    SELECT 
        data,
        COUNT(*) as linhas_dia,
        SUM(TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE)) as fat_dia,
        SUM(qtd_vendas_estimadas_dia) as vendas_dia
    FROM '{pq_path.replace(os.sep, "/")}'
    WHERE data = '{max_data}'
    GROUP BY data
    """
    df_dia = con.execute(q_dia).df()
    ultimo_dia = df_dia.iloc[0].to_dict() if len(df_dia) > 0 else {}

    q_ufs = f"""
    SELECT 
        uf,
        SUM(TRY_CAST(REPLACE(REPLACE(faturamento_estimado_dia, '.', ''), ',', '.') AS DOUBLE)) as fat_uf
    FROM '{pq_path.replace(os.sep, "/")}'
    WHERE ano = {ano_atual}
    GROUP BY uf
    ORDER BY fat_uf DESC
    LIMIT 5
    """
    df_ufs = con.execute(q_ufs).df()
    top_ufs = df_ufs.to_dict(orient="records")

    return {
        "max_data": max_data,
        "ano_atual": ano_atual,
        "mes_atual": mes_atual,
        "totais": totais,
        "ultimo_dia": ultimo_dia,
        "top_ufs": top_ufs
    }


def montar_mensagem(dados):
    """Monta a mensagem executiva formatada para o Telegram."""
    max_data_raw = dados["max_data"]
    if hasattr(max_data_raw, "strftime"):
        data_formatada = max_data_raw.strftime("%d/%m/%Y")
    else:
        try:
            data_formatada = datetime.strptime(str(max_data_raw)[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            data_formatada = str(max_data_raw)

    tot = dados["totais"]
    dia = dados["ultimo_dia"]
    ufs = dados["top_ufs"]
    ano = dados["ano_atual"]
    mes_num = dados["mes_atual"]
    meses_pt = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    nome_mes = meses_pt[mes_num] if 1 <= mes_num <= 12 else f"Mês {mes_num}"

    fat_hist = tot.get("faturamento_total_historico", 0)
    fat_ytd = tot.get("faturamento_ytd", 0)
    fat_mtd = tot.get("faturamento_mtd", 0)
    fat_dia = dia.get("fat_dia", 0)
    vendas_dia = int(dia.get("vendas_dia", 0) or 0)

    linhas_ufs = []
    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    for i, item in enumerate(ufs):
        em = emojis[i] if i < len(emojis) else "▫️"
        linhas_ufs.append(f"{em} *{item['uf']}:* {formatar_milhoes_bi(item['fat_uf'])}")
    bloco_ufs = "\n".join(linhas_ufs)

    mensagem = (
        f"🏙️ *ML CIDADES · BOLETIM EXECUTIVO (11:00)* 🏙️\n"
        f"📅 *Posição da Base:* {data_formatada}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *TOTAL FATURADO ATÉ O MOMENTO:*\n\n"
        f"🌐 *Total Geral Acumulado:*\n"
        f"   └ *{formatar_moeda(fat_hist)}* _({formatar_milhoes_bi(fat_hist)})_\n\n"
        f"📈 *Ano Vigente ({ano} · YTD):*\n"
        f"   └ *{formatar_moeda(fat_ytd)}* _({formatar_milhoes_bi(fat_ytd)})_\n\n"
        f"🗓️ *Mês Vigente ({nome_mes} · MTD):*\n"
        f"   └ *{formatar_moeda(fat_mtd)}* _({formatar_milhoes_bi(fat_mtd)})_\n\n"
        f"⚡ *Último Dia Fechado ({data_formatada}):*\n"
        f"   └ *{formatar_moeda(fat_dia)}* _({formatar_milhoes_bi(fat_dia)})_\n"
        f"   └ *Volume:* {vendas_dia:,} vendas estimadas\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 *TOP 5 ESTADOS EM {ano}:*\n"
        f"{bloco_ufs}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 _Mensagem automática gerada pelo Bot ML Cidades / Joca Hub._"
    )
    return mensagem


def enviar_telegram(texto):
    """Envia a mensagem para o chat do Telegram configurado com retries."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": texto,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    logger.info(f"Enviando mensagem para chat_id: {TELEGRAM_CHAT_ID}...")
    
    for tentativa in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=25)
            if resp.status_code == 200:
                logger.info(" Mensagem enviada com sucesso ao Telegram!")
                return True
            else:
                logger.warning(f"Tentativa {tentativa} falhou: HTTP {resp.status_code} - {resp.text}")
        except Exception as e:
            logger.warning(f"Tentativa {tentativa} erro de conexão: {e}")
        time.sleep(3)

    logger.error("❌ Todas as tentativas de envio ao Telegram falharam.")
    return False


def main():
    logger.info("=" * 65)
    logger.info(" INICIANDO ROTINA DE ALERTA DIÁRIO - ML CIDADES (11:00 BRT)")
    logger.info("=" * 65)

    client = obter_cliente_bigquery()
    dados = None

    if client:
        try:
            dados = consultar_metricas_cidades(client)
        except Exception as e:
            logger.error(f"Erro ao consultar BigQuery: {e}")

    if not dados:
        logger.info("Tentando fallback via DuckDB local...")
        dados = fallback_duckdb_local()

    if not dados:
        logger.error("❌ Não foi possível obter dados nem do BigQuery nem localmente.")
        sys.exit(1)

    msg = montar_mensagem(dados)
    print("\n--- PRÉVIA DA MENSAGEM DO TELEGRAM ---\n")
    print(msg)
    print("\n---------------------------------------\n")

    sucesso = enviar_telegram(msg)
    if sucesso:
        logger.info(" Rotina finalizada com sucesso total!")
    else:
        logger.error("❌ Falha no envio do alerta.")
        sys.exit(1)


if __name__ == "__main__":
    main()
