"""
Módulo de Atualização Incremental: ML_Cidades (Distribuição por Municípios)
-------------------------------------------------------------------------
Executado após a carga diária da base nacional no BigQuery.
IMPORTANTE: Processa APENAS o lote novo (dia de hoje / dias faltantes),
NUNCA reprocessa o histórico consolidado.
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

logger = logging.getLogger("MercadoLivrePipeline.Cidades")

def parse_num(v):
    if pd.isna(v): 
        return 0.0
    s = str(v).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

def obter_dimensao_municipios(client=None, script_dir=None):
    candidatos = []
    if script_dir:
        candidatos.append(os.path.join(script_dir, "Dim_Municipios_e_Pop.parquet"))
    candidatos.append(r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Dim_Municipios_e_Pop.parquet")
    candidatos.append(r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\Atualizador_BigQuery_MercadoLivre\Dim_Municipios_e_Pop.parquet")
    
    for c in candidatos:
        if os.path.exists(c):
            logger.info(f"[ML_CIDADES] Carregando dimensao local: {c}")
            return pd.read_parquet(c)
            
    if client:
        logger.info("[ML_CIDADES] Carregando dimensao direto do BigQuery ML_Cidades.Dim_Municipios_e_Pop...")
        q = "SELECT * FROM `mercado-livre-mais-vendidos.ML_Cidades.Dim_Municipios_e_Pop`"
        return client.query(q).to_dataframe()
        
    raise FileNotFoundError("Dim_Municipios_e_Pop nao encontrada localmente nem no BigQuery.")

def atualizar_cidades_incremental(df_lote=None, bq_client=None, project_id="mercado-livre-mais-vendidos"):
    tabela_cidades = f"{project_id}.ML_Cidades.Fato_MercadoLivre_MaisVendidos"
    
    if df_lote is None or len(df_lote) == 0:
        logger.info("[ML_CIDADES] Verificando datas pendentes no BigQuery...")
        q_pend = f"""
        WITH bq_nac AS (
            SELECT data, COUNT(*) as c_nac
            FROM `{project_id}.Mercado_Livre.Fato_MercadoLivre_MaisVendidos`
            WHERE data >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
            GROUP BY data
        ),
        bq_cid AS (
            SELECT data, COUNT(*) as c_cid
            FROM `{tabela_cidades}`
            WHERE data >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
            GROUP BY data
        )
        SELECT n.data
        FROM bq_nac n
        LEFT JOIN bq_cid c ON n.data = c.data
        WHERE c.c_cid IS NULL OR (c.c_cid < 500 AND n.c_nac >= 200)
        ORDER BY n.data
        """
        datas_pend = [r.data for r in bq_client.query(q_pend).result()]
        if not datas_pend:
            logger.info("[ML_CIDADES] Nenhuma data pendente na base ML_Cidades. Tudo 100% atualizado.")
            return True
            
        datas_str = "', '".join([d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in datas_pend])
        logger.info(f"[ML_CIDADES] Puxando dados nacionais para as datas pendentes: {datas_str}...")
        q_src = f"""
        SELECT *
        FROM `{project_id}.Mercado_Livre.Fato_MercadoLivre_MaisVendidos`
        WHERE data IN ('{datas_str}')
        ORDER BY data, posicao_ranking
        """
        df_lote = bq_client.query(q_src).to_dataframe()

    if df_lote is None or len(df_lote) == 0:
        logger.info("[ML_CIDADES] Nenhum registro a processar.")
        return True

    script_dir = os.path.dirname(os.path.abspath(__file__))
    df_dim = obter_dimensao_municipios(client=bq_client, script_dir=script_dir)
    
    pop_pct = df_dim['populacao_pct'].values
    porte = df_dim['porte_municipio'].values
    regiao = df_dim['regiao_macro'].values
    regional = df_dim['regional'].values
    cod_ibge = df_dim['cod_ibge'].values
    municipio = df_dim['municipio'].values
    uf = df_dim['uf'].values
    
    fator_renda = np.ones(len(df_dim))
    for i in range(len(df_dim)):
        p = porte[i]
        r = regional[i]
        if r == 'SP CAP':
            fator_renda[i] = 1.70
        elif p == 'Metrópole / Grande':
            fator_renda[i] = 1.45
        elif p == 'Médio Porte':
            fator_renda[i] = 1.15
        elif p == 'Pequeno Porte I':
            fator_renda[i] = 0.85
        else:
            fator_renda[i] = 0.50
            
    df_lote = df_lote.copy()
    if 'dt' not in df_lote.columns:
        df_lote['dt'] = pd.to_datetime(df_lote['data'])
    if 'preco_float' not in df_lote.columns:
        df_lote['preco_float'] = df_lote['preco_atual'].apply(parse_num)
        
    dias_processar = sorted(df_lote['dt'].unique())
    logger.info(f"[ML_CIDADES] Processando distribuicao incremental para {len(dias_processar)} dia(s): {[d.strftime('%Y-%m-%d') for d in dias_processar]}")
    
    for dia_dt in dias_processar:
        dia_str = dia_dt.strftime('%Y-%m-%d')
        seed = int(dia_dt.strftime('%Y%m%d'))
        rng = np.random.default_rng(seed)
        
        df_dia = df_lote[df_lote['dt'] == dia_dt]
        rows_dia = []
        vendas_dia_total = 0
        fat_dia_total = 0.0
        
        for _, row in df_dia.iterrows():
            vendas = int(row['qtd_vendas_estimadas_dia']) if pd.notna(row['qtd_vendas_estimadas_dia']) else 0
            if vendas <= 0:
                continue
                
            preco = float(row['preco_float'])
            cat = str(row['categoria']) if pd.notna(row['categoria']) else ''
            titulo = str(row['titulo_produto']).lower() if pd.notna(row['titulo_produto']) else ''
            
            fator_clima = np.ones(len(df_dim))
            if any(k in titulo for k in ['ventilador', 'ar condicionado', 'climatizador']):
                fator_clima[np.isin(regiao, ['Norte', 'Nordeste', 'Centro-Oeste'])] *= 1.8
                fator_clima[regiao == 'Sul'] *= 0.5
            elif any(k in titulo for k in ['aquecedor', 'inverno', 'térmico', 'termico']):
                fator_clima[regiao == 'Sul'] *= 2.5
                fator_clima[np.isin(regiao, ['Norte', 'Nordeste'])] *= 0.1
            elif cat == 'Ferramentas e Construção':
                fator_clima[np.isin(regiao, ['Centro-Oeste', 'Sul'])] *= 1.3
                fator_clima[regional == 'SP INT'] *= 1.3
                fator_clima[regional == 'SP CAP'] *= 0.7
            elif cat == 'Informática':
                fator_clima[porte == 'Metrópole / Grande'] *= 1.35
                fator_clima[porte == 'Pequeno Porte II'] *= 0.6
                
            fator_ticket = np.ones(len(df_dim))
            if preco > 2500:
                fator_ticket[porte == 'Metrópole / Grande'] *= 1.5
                fator_ticket[porte == 'Pequeno Porte II'] *= 0.25
            elif preco < 150:
                fator_ticket[porte == 'Pequeno Porte II'] *= 1.3
                
            w = pop_pct * fator_renda * fator_clima * fator_ticket
            p = w / w.sum()
            
            dist_vendas = rng.multinomial(vendas, p)
            cidades_com_venda = np.where(dist_vendas > 0)[0]
            data_bq = dia_dt.date()
            
            for idx in cidades_com_venda:
                qtd_cidade = int(dist_vendas[idx])
                fat_cidade = round(qtd_cidade * preco, 2)
                vendas_dia_total += qtd_cidade
                fat_dia_total += fat_cidade
                
                rows_dia.append({
                    'data': data_bq,
                    'ano': int(row['ano']),
                    'mes': int(row['mes']),
                    'ano_mes': str(row['ano_mes']),
                    'posicao_ranking': int(row['posicao_ranking']),
                    'categoria': str(row['categoria']) if pd.notna(row['categoria']) else None,
                    'subcategoria': str(row['subcategoria']) if pd.notna(row['subcategoria']) else None,
                    'id_anuncio': str(row['id_anuncio']) if pd.notna(row['id_anuncio']) else None,
                    'titulo_produto': str(row['titulo_produto']) if pd.notna(row['titulo_produto']) else None,
                    'marca': str(row['marca']) if pd.notna(row['marca']) else None,
                    'preco_atual': str(row['preco_atual']) if pd.notna(row['preco_atual']) else None,
                    'preco_original': str(row['preco_original']) if pd.notna(row['preco_original']) else None,
                    'desconto_pct': str(row['desconto_pct']) if pd.notna(row['desconto_pct']) else None,
                    'parcelamento': str(row['parcelamento']) if pd.notna(row['parcelamento']) else None,
                    'qtd_vendas_estimadas_dia': qtd_cidade,
                    'faturamento_estimado_dia': str(fat_cidade).replace('.', ','),
                    'avaliacao_nota': str(row['avaliacao_nota']) if pd.notna(row['avaliacao_nota']) else None,
                    'qtd_avaliacoes': int(row['qtd_avaliacoes']) if pd.notna(row['qtd_avaliacoes']) else None,
                    'is_full': bool(row['is_full']) if pd.notna(row['is_full']) else False,
                    'frete_gratis': bool(row['frete_gratis']) if pd.notna(row['frete_gratis']) else False,
                    'loja_oficial': str(row['loja_oficial']) if pd.notna(row['loja_oficial']) else None,
                    'reputacao_vendedor': str(row['reputacao_vendedor']) if pd.notna(row['reputacao_vendedor']) else None,
                    'url_imagem': str(row['url_imagem']) if pd.notna(row['url_imagem']) else None,
                    'url_produto': str(row['url_produto']) if pd.notna(row['url_produto']) else None,
                    'tipo_dado': 'Sintético - Cidades',
                    'cod_ibge': str(cod_ibge[idx]),
                    'municipio': str(municipio[idx]),
                    'uf': str(uf[idx]),
                    'regiao': str(regiao[idx])
                })
                
        df_dia_cidades = pd.DataFrame(rows_dia)
        logger.info(f"[ML_CIDADES] Dia {dia_str}: {len(df_dia_cidades):,} linhas geradas | Vendas: {vendas_dia_total:,} | Fat: R$ {fat_dia_total:,.2f}")
        
        logger.info(f"[ML_CIDADES] Limpando dados anteriores de {dia_str} em ML_Cidades (se existirem)... via query truncate...")
        try:
            clean_query = f"SELECT * FROM `{tabela_cidades}` WHERE data != '{dia_str}'"
            clean_config = bigquery.QueryJobConfig(
                destination=tabela_cidades,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            bq_client.query(clean_query, job_config=clean_config).result()
        except Exception as e:
            logger.warning(f"[ML_CIDADES] Aviso na limpeza de {dia_str}: {e}")
            
        logger.info(f"[ML_CIDADES] Gravando {len(df_dia_cidades):,} linhas de {dia_str} no BigQuery...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = bq_client.load_table_from_dataframe(df_dia_cidades, tabela_cidades, job_config=job_config)
        load_job.result()
        logger.info(f"[ML_CIDADES] [✓] Dia {dia_str} gravado com sucesso!")
        
    return True

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    from google.oauth2 import service_account
    key_path = r'C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Mercado_Livre\JSON\mercado-livre-mais-vendidos-143cf1ecac17.json'
    cred = service_account.Credentials.from_service_account_file(key_path) if os.path.exists(key_path) else None
    client = bigquery.Client(project='mercado-livre-mais-vendidos', credentials=cred)
    atualizar_cidades_incremental(bq_client=client)
