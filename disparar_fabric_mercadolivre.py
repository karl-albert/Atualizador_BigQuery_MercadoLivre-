# -*- coding: utf-8 -*-
"""
==================================================================================================
DISPARADOR DO PIPELINE DO MERCADO LIVRE NO MICROSOFT FABRIC (VIA REST API / WEBHOOK)
==================================================================================================
Ambiente Destino: Microsoft Fabric
Workspace: KAC (0c2a1a5e-4519-4e4e-b3e6-14288c11291d)
Lakehouse: LH_MercadoLivre
Modelo Semântico: SM_MercadoLivre (Direct Lake)
==================================================================================================
"""
import os
import sys
import json
import requests

# Forçar encoding UTF-8 no console do Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Variáveis de ambiente configuráveis (GitHub Secrets ou Local)
tenant_id = os.environ.get("AZURE_TENANT_ID")
client_id = os.environ.get("AZURE_CLIENT_ID")
client_secret = os.environ.get("AZURE_CLIENT_SECRET")
webhook_url = os.environ.get("FABRIC_WEBHOOK_URL")

# IDs Oficiais do Workspace KAC e Pipeline Mercado Livre
workspace_id = os.environ.get("WORKSPACE_ID", "0c2a1a5e-4519-4e4e-b3e6-14288c11291d")
pipeline_id = os.environ.get("PIPELINE_ID_MELI", os.environ.get("PIPELINE_ID", ""))
dataset_id = os.environ.get("DATASET_ID_MELI", "") # SM_MercadoLivre se for refresh direto

print("=" * 75)
print("  🚀 DISPARADOR AUTOMÁTICO DE ATUALIZAÇÃO NO MICROSOFT FABRIC")
print("  Projeto: Mercado Livre — Inteligência & Mais Vendidos")
print(f"  Workspace ID: {workspace_id}")
if pipeline_id:
    print(f"  Pipeline ID : {pipeline_id}")
print("=" * 75)

# -------------------------------------------------------------------------
# MÉTODO 1: Disparo via Webhook seguro do Power Automate (Zero credencial)
# -------------------------------------------------------------------------
if webhook_url and webhook_url.strip():
    print("\n[*] Disparando execução via Webhook seguro do Power Automate...")
    try:
        payload = {
            "projeto": "Mercado Livre",
            "origem": "GitHub Actions / Automação Central",
            "workspace_id": workspace_id,
            "pipeline_id": pipeline_id
        }
        r = requests.post(webhook_url.strip(), json=payload, timeout=30)
        print(f"[*] Resposta Webhook: HTTP {r.status_code}")
        if r.status_code in [200, 202]:
            print("✅ Sucesso! O Pipeline do Mercado Livre foi iniciado no Fabric via Webhook.")
            sys.exit(0)
        else:
            print(f"⚠️ Aviso no Webhook: {r.text}")
    except Exception as e:
        print(f"⚠️ Erro ao chamar Webhook: {e}")

# -------------------------------------------------------------------------
# MÉTODO 2: Disparo via Service Principal do Azure Entra ID (REST API Oficial)
# -------------------------------------------------------------------------
if client_id and client_secret and tenant_id:
    print("\n[*] Autenticando no Azure Entra ID (Service Principal)...")
    token_url = f"https://login.microsoftonline.com/{tenant_id.strip()}/oauth2/v2.0/token"
    token_data = {
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default"
    }
    try:
        r_token = requests.post(token_url, data=token_data, timeout=15)
        if r_token.status_code == 200:
            token = r_token.json().get("access_token")
            print("✅ Token Azure AD obtido com sucesso!")

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            # 2.1 Disparar Pipeline se pipeline_id configurado
            if pipeline_id:
                print(f"[*] Executando Data Pipeline no Fabric: {pipeline_id}...")
                fabric_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
                r_pipe = requests.post(fabric_url, headers=headers, timeout=20)
                print(f"[*] Resposta Fabric API: HTTP {r_pipe.status_code}")
                if r_pipe.status_code in [200, 202]:
                    loc = r_pipe.headers.get("Location", "")
                    print(f"✅ Pipeline disparado com sucesso no Fabric! Instância: {loc}")
                    sys.exit(0)
                else:
                    print(f"⚠️ Falha ao executar pipeline: {r_pipe.status_code} - {r_pipe.text}")

            # 2.2 Disparar Refresh do Modelo Semântico SM_MercadoLivre se dataset_id configurado
            if dataset_id:
                print(f"[*] Forçando refresh de framing do Direct Lake no SM_MercadoLivre...")
                pbi_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
                r_ref = requests.post(pbi_url, headers=headers, json={"type": "Automatic"}, timeout=20)
                if r_ref.status_code in [200, 202]:
                    print("✅ Refresh do SM_MercadoLivre disparado com sucesso via REST API!")
                    sys.exit(0)

        else:
            print(f"⚠️ Erro ao autenticar no Azure: HTTP {r_token.status_code} - {r_token.text}")
    except Exception as e_az:
        print(f"⚠️ Exceção ao conectar no Azure Entra ID: {e_az}")

# -------------------------------------------------------------------------
# MODO INFORMATIVO / AGENDAMENTO NATIVO
# -------------------------------------------------------------------------
print("\n" + "=" * 75)
print("  ℹ️ INFORMAÇÃO OPERACIONAL — AGENDAMENTO NATIVO FABRIC")
print("=" * 75)
print("""
O Microsoft Fabric possui suporte a agendamento automático nativo (Schedule),
exatamente como no BigQuery / GitHub Actions:
  
  1. No Fabric Workspace KAC, abra o pipeline 'Pipeline_ETL_MercadoLivre'.
  2. Clique em 'Schedule' (Agendamento) no menu superior.
  3. Ative a recorrência diária nos 3 horários oficiais:
       - 08:15 BRT (15 min após a abertura)
       - 18:15 BRT (15 min após o pico comercial)
       - 23:15 BRT (15 min após o fechamento do dia)
       
Dessa forma, o Fabric atualiza sozinho mesmo sem chamadas externas via API!
""")
