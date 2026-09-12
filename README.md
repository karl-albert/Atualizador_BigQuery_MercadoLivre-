# Mercado Livre Intelligence — Pipeline Automatizado BigQuery (3x ao Dia)

Pipeline automatizado em nuvem para monitoramento diário e intradiário dos 50 produtos mais vendidos das 5 principais categorias do Mercado Livre Brasil, com carga incremental idempotente no Google BigQuery.

---

## ⏰ Cronograma das 3 Atualizações Diárias:

O workflow está configurado no GitHub Actions com fuso horário ajustado para o **Horário de Brasília (BRT / UTC-3)**:

| Rodada | Horário Brasília (BRT) | Horário GitHub (UTC) | Objetivo Analítico |
| :--- | :---: | :---: | :--- |
| **1ª Rodada (Abertura)** | **08h00** | `11h00 UTC` | Captura o ranking matinal de abertura e as primeiras vendas do dia |
| **2ª Rodada (Pico Comercial)** | **18h00** | `21h00 UTC` | Captura o fechamento comercial, promoções relâmpago e movimentação da tarde |
| **3ª Rodada (Fechamento)** | **23h00** | `02h00 UTC` *(+1)* | Consolidação final e definitiva do faturamento e volume diário |

> **Garantia de Idempotência**: A cada rodada do mesmo dia, o script remove o snapshot intermediário anterior daquela data e insere a posição mais recente, garantindo que o Power BI sempre veja exatamente 1 registro consolidado por produto/dia sem duplicar valores nem inflar faturamento.

---

## 🚀 Como Configurar no GitHub Actions:

1. Suba ou sincronize estes arquivos no seu repositório do GitHub:
   - `atualizar_mercadolivre.py`
   - `requirements.txt`
   - `.github/workflows/rotina_mercadolivre.yml`
2. No GitHub, vá em **Settings** -> **Secrets and variables** -> **Actions** -> **New repository secret**:
   - `GCP_SA_KEY`: Cole o conteúdo JSON da sua chave de Conta de Serviço do Google Cloud.
   - `GCP_PROJECT_ID`: `mercado-livre-mais-vendidos`
3. Na aba **Actions**, o fluxo já estará ativo com os 3 agendamentos automáticos diários. Você também pode disparar a qualquer momento clicando em **Run workflow**.
