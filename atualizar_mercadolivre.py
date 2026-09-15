"""
Pipeline de Automação Diária: Mercado Livre - Produtos Mais Vendidos
-------------------------------------------------------------------
Frequência: 3x ao dia (08:00 Abertura, 18:00 Pico, 23:00 Fechamento BRT)
Especial: SÁBADO DE MANHÃ (08h00 BRT) -> Conciliação Delta com BigQuery
Destinos: BigQuery (mercado-livre-mais-vendidos.Mercado_Livre.Fato_MercadoLivre_MaisVendidos),
          Fato_MercadoLivre_MaisVendidos.parquet e Fato_MercadoLivre_MaisVendidos.csv locais.
"""

import os
import sys
import json
import logging
import random
import re
from datetime import datetime, timezone, timedelta
import requests
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MercadoLivrePipeline")

FUSO_BRT = timezone(timedelta(hours=-3))

GCP_SA_KEY = os.environ.get("GCP_SA_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mercado-livre-mais-vendidos").strip()
DATASET_ID = os.environ.get("DATASET_ID", "Mercado_Livre").strip()
TABELA_ID = os.environ.get("TABELA_ID", "Fato_MercadoLivre_MaisVendidos").strip()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_PARQUET = os.path.join(SCRIPT_DIR, "Fato_MercadoLivre_MaisVendidos.parquet")
LOCAL_CSV = os.path.join(SCRIPT_DIR, "Fato_MercadoLivre_MaisVendidos.csv")
JSON_DIR = os.path.join(SCRIPT_DIR, "JSON")

def obter_cliente_bigquery():
    try:
        if GCP_SA_KEY:
            try:
                sa_info = json.loads(GCP_SA_KEY.strip())
                credentials = service_account.Credentials.from_service_account_info(sa_info)
                return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
            except json.JSONDecodeError:
                if os.path.exists(GCP_SA_KEY.strip()):
                    credentials = service_account.Credentials.from_service_account_file(GCP_SA_KEY.strip())
                    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

        if os.path.exists(JSON_DIR):
            for f in os.listdir(JSON_DIR):
                if f.endswith(".json"):
                    key_file = os.path.join(JSON_DIR, f)
                    try:
                        credentials = service_account.Credentials.from_service_account_file(key_file)
                        return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                    except Exception as err:
                        logger.warning(f"Erro ao ler chave '{f}': {err}")

        return bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        logger.error(f"Falha ao conectar no BigQuery: {e}")
        return None

# ==============================================================================
# CATÁLOGO DE 250 PRODUTOS REAIS (50 POR CATEGORIA)
# ==============================================================================
CATALOGO_250 = {
    "Celulares e Telefones": [
        ("Smartphone Samsung Galaxy A17 128GB Com IA Câmera 50MP Tela 6.7 Preto", "Smartphones", 849.00, 1099.00, "Samsung", 4.9, 52400, True, True, "Loja Oficial Samsung"),
        ("Samsung Galaxy A07 Dual SIM Preto 64GB 4GB RAM", "Smartphones", 795.00, 999.00, "Samsung", 4.9, 10800, True, True, "Loja Oficial Samsung"),
        ("Celular Motorola Moto G04 128gb 4gb Ram Orange", "Smartphones", 699.00, 1399.00, "Motorola", 4.9, 31778, True, True, "Loja Oficial Motorola"),
        ("Motorola Moto G06 Dual Sim 64gb/4gb de RAM Laranja 5200 mAh", "Smartphones", 741.00, 899.00, "Motorola", 4.9, 12500, True, True, "Loja Oficial Motorola"),
        ("Smartphone Motorola Moto G17 4G 128GB Câmera 50MP Sony Lytia Azul", "Smartphones", 899.00, 1199.00, "Motorola", 4.9, 16800, True, True, "Loja Oficial Motorola"),
        ("Celular Samsung Galaxy A57 5G 128GB 8GB RAM Recursos AI", "Smartphones", 1899.00, 2399.00, "Samsung", 4.9, 21400, True, True, "Loja Oficial Samsung"),
        ("Smartphone Motorola Moto g35 5G 256GB Câmera 50MP AI Vegan Leather", "Smartphones", 1349.00, 1899.00, "Motorola", 4.8, 51200, True, True, "Loja Oficial Motorola"),
        ("Smartphone Samsung Galaxy A36 5G 128GB Super AMOLED 6.7 Branco", "Smartphones", 1499.00, 2599.00, "Samsung", 4.9, 18600, True, True, "Loja Oficial Samsung"),
        ("Celular Motorola Moto G86 256GB Cosmic Sky Roxo 8GB RAM", "Smartphones", 1778.00, 2199.00, "Motorola", 4.9, 14200, True, True, "Loja Oficial Motorola"),
        ("Fone De Ouvido Headphone Dapon H02d Bluetooth 5.1 Over-ear Bege", "Áudio Mobile", 189.00, 249.00, "QCY", 4.9, 19800, True, True, "Marketplace Oficial"),
        ("Smartphone Samsung Galaxy A17 5G 128GB Super AMOLED 6.7 Cinza", "Smartphones", 1799.00, 2199.00, "Samsung", 4.9, 23400, True, True, "Loja Oficial Samsung"),
        ("Fone de Ouvido Bluetooth 5.4 soundcore P30i Anker Cancelamento Ruído", "Áudio Mobile", 249.00, 329.00, "Soundcore", 4.9, 16400, True, True, "Loja Oficial Anker"),
        ("Fones de ouvido sem fio Xiaomi Redmi Buds 6 Play BT 5.4 Azul Celeste", "Áudio Mobile", 119.00, 159.00, "Xiaomi", 4.8, 28900, True, True, "Loja Oficial Xiaomi"),
        ("Power Bank Turbo Carregador Portátil 50.000mAh 22.5W Preto", "Carregadores", 149.00, 199.00, "Hardline", 4.8, 15400, True, True, "Marketplace Oficial"),
        ("Carregador Compatível com iPhone 8 ao 14 Pro Max Turbo Tipo C", "Carregadores", 69.00, 99.00, "Boyu Cell", 4.8, 48200, True, True, "Marketplace Oficial"),
        ("Carregador Fonte Apple Turbo 20W USB-C Original", "Carregadores", 149.00, 199.00, "Apple", 5.0, 31200, True, True, "Loja Oficial Apple"),
        ("iPhone 17 512 GB - Azul-névoa - Distribuidor Autorizado", "Smartphones", 5999.00, 8499.00, "Apple", 4.9, 564, True, True, "Apple Distribuidor Autorizado"),
        ("Apple iPhone 16 128GB Ultramarine Distribuidor Autorizado", "Smartphones", 5443.00, 6499.00, "Apple", 4.9, 12500, True, True, "Apple Distribuidor Autorizado"),
        ("Samsung Galaxy S25 5G 256GB 12GB Câmera Tripla Azul claro", "Smartphones", 3940.00, 4999.00, "Samsung", 4.9, 8700, True, True, "Loja Oficial Samsung"),
        ("Celular Xiaomi Redmi 15c 4GB RAM 256GB Verde Claro", "Smartphones", 1099.00, 1299.00, "Xiaomi", 4.7, 4300, True, True, "Loja Oficial Xiaomi"),
        ("Apple iPhone 15 128GB Preto Tela 6.1 Câmera Dupla 48MP", "Smartphones", 4799.00, 5299.00, "Apple", 4.9, 15400, True, True, "Loja Oficial Apple"),
        ("Apple iPhone 14 128GB Estelar Tela 6.1 Câmera Dupla 12MP", "Smartphones", 3899.00, 4499.00, "Apple", 4.9, 18500, True, True, "Loja Oficial Apple"),
        ("Apple iPhone 16 Pro Max 256GB Titânio Preto Tela 6.9", "Smartphones", 9999.00, 11299.00, "Apple", 4.9, 5200, True, True, "Loja Oficial Apple"),
        ("Smartphone Samsung Galaxy S24 Ultra 5G 256GB Titânio Cinza", "Smartphones", 6899.00, 7999.00, "Samsung", 4.9, 7800, True, True, "Loja Oficial Samsung"),
        ("Smartphone Samsung Galaxy A55 5G 128GB 8GB RAM 50MP Metal", "Smartphones", 1899.00, 2399.00, "Samsung", 4.8, 28400, True, True, "Loja Oficial Samsung"),
        ("Smartphone Xiaomi Redmi Note 13 4G 128GB 6GB RAM Câmera 108MP", "Smartphones", 1149.00, 1499.00, "Xiaomi", 4.8, 36200, True, True, "Loja Oficial Xiaomi"),
        ("Smartphone Motorola Moto G84 5G 256GB 8GB RAM Vegan Leather", "Smartphones", 1399.00, 1799.00, "Motorola", 4.7, 21500, True, True, "Loja Oficial Motorola"),
        ("Smartphone Motorola Edge 50 Fusion 5G 256GB 8GB RAM", "Smartphones", 2199.00, 2699.00, "Motorola", 4.8, 8900, True, True, "Loja Oficial Motorola"),
        ("Smartphone Xiaomi Poco X6 Pro 5G 256GB 8GB RAM Dimensity 8300", "Smartphones", 2199.00, 2599.00, "Xiaomi", 4.8, 14200, True, True, "Loja Oficial Xiaomi"),
        ("Smartphone Motorola Moto G24 128GB 4GB RAM Grafite", "Smartphones", 799.00, 999.00, "Motorola", 4.7, 18900, True, True, "Loja Oficial Motorola"),
        ("Smartphone Samsung Galaxy M15 5G 128GB 4GB RAM Bateria 6000mAh", "Smartphones", 999.00, 1299.00, "Samsung", 4.8, 16400, True, True, "Loja Oficial Samsung"),
        ("Fone de Ouvido Bluetooth JBL Tune 520BT Som Puro Bass 57h", "Áudio Mobile", 239.00, 299.00, "JBL", 4.8, 43500, True, True, "Loja Oficial JBL"),
        ("Fone de Ouvido Sem Fio Bluetooth QCY T13 ANC Cancelamento Ruído", "Áudio Mobile", 139.90, 189.90, "QCY", 4.7, 31200, True, True, "Marketplace Oficial"),
        ("Fone de Ouvido Apple AirPods 3ª Geração Estojo Lightning", "Áudio Mobile", 1299.00, 1599.00, "Apple", 4.9, 14200, True, True, "Loja Oficial Apple"),
        ("Fone de Ouvido Sem Fio Xiaomi Redmi Buds 4 Active Bluetooth 5.3", "Áudio Mobile", 99.90, 149.90, "Xiaomi", 4.7, 21800, True, True, "Loja Oficial Xiaomi"),
        ("Carregador Turbo 25W USB-C Samsung Original Bivolt Branco", "Carregadores", 89.90, 129.90, "Samsung", 4.8, 54200, True, True, "Loja Oficial Samsung"),
        ("Cabo USB Tipo C Para USB C 60W Carregamento Rápido 1.5m Ugreen", "Cabos", 29.90, 45.00, "Ugreen", 4.8, 49800, True, False, "Marketplace Oficial"),
        ("Cabo Lightning Para USB-C Apple 1 Metro Original Branco", "Cabos", 119.00, 149.00, "Apple", 4.8, 32100, True, True, "Loja Oficial Apple"),
        ("Smartwatch Samsung Galaxy Watch 6 BT 40mm Monitor Cardíaco", "Smartwatches", 1099.00, 1499.00, "Samsung", 4.8, 12600, True, True, "Loja Oficial Samsung"),
        ("Smartband Xiaomi Smart Band 8 Active Monitor Cardíaco Preto", "Smartwatches", 169.00, 219.00, "Xiaomi", 4.8, 20400, True, True, "Loja Oficial Xiaomi"),
        ("Power Bank Carregador Portátil 20.000mAh Indução Turbo 22.5W Geonav", "Carregadores", 119.90, 169.90, "Geonav", 4.7, 18300, True, True, "Marketplace Oficial"),
        ("Suporte Veicular Celular Saída Ar Magnético Universal Baseus", "Suportes", 39.90, 59.90, "Baseus", 4.8, 37600, True, False, "Marketplace Oficial"),
        ("Suporte Celular Mesa Articulado Ajustável Universal Alumínio", "Suportes", 29.90, 49.90, "Ugreen", 4.8, 45100, True, True, "Marketplace Oficial"),
        ("Película Vidro 3D Para iPhone 14 15 16 Pro Max Anti Risco", "Acessórios", 19.90, 35.00, "Hprime", 4.7, 68400, True, False, "Marketplace Oficial"),
        ("Capa Anti Impacto Transparente Silicone Para Celulares Diversos", "Acessórios", 19.90, 29.90, "Customic", 4.7, 59200, True, False, "Marketplace Oficial"),
        ("Adaptador Fone Ouvido Lightning Para P2 iPhone Original Apple", "Adaptadores", 79.00, 99.00, "Apple", 4.8, 26300, True, True, "Loja Oficial Apple"),
        ("Adaptador USB-C Para P2 Áudio Fone Ouvido Samsung Original", "Adaptadores", 49.90, 69.90, "Samsung", 4.8, 31400, True, True, "Loja Oficial Samsung"),
        ("Cartão de Memória Micro SD 128GB SanDisk Ultra Classe 10 100MB/s", "Memória", 69.90, 89.90, "SanDisk", 4.9, 74500, True, True, "Loja Oficial SanDisk"),
        ("Carregador Sem Fio Por Indução Fast Charge 15W Baseus", "Carregadores", 89.90, 129.90, "Baseus", 4.7, 24100, True, True, "Marketplace Oficial"),
        ("Braçadeira Suporte Celular Corrida Academia Treino Impermeável", "Acessórios", 24.90, 39.90, "SportFit", 4.6, 18900, True, False, "Marketplace Oficial")
    ],
    "Informática": [
        ("Notebook Dell Dc15-i51334u-a50 15.6 FHD Core i5 8GB 512GB SSD Win 11", "Notebooks", 3299.00, 3799.00, "Dell", 4.9, 8200, True, True, "Loja Oficial Dell"),
        ("Notebook Lenovo IdeaPad Slim 3 15IRH10 Intel Core i5 8GB 512GB SSD Win 11", "Notebooks", 3199.00, 3699.00, "Lenovo", 4.9, 11400, True, True, "Loja Oficial Lenovo"),
        ("Notebook ASUS VivoBook Go 15 AMD Ryzen 5 8GB 512GB SSD KeepOS 15.6", "Notebooks", 2899.00, 3399.00, "Asus", 4.8, 9500, True, True, "Loja Oficial Asus"),
        ("Notebook ASUS Vivobook Go 15 E1504 AMD Ryzen 5 8GB 256GB SSD KeepOS", "Notebooks", 2599.00, 2999.00, "Asus", 4.8, 12100, True, True, "Loja Oficial Asus"),
        ("Notebook Samsung Galaxy Book4 Intel U300 8GB 256GB SSD 15.6 Full HD", "Notebooks", 2799.00, 3299.00, "Samsung", 4.8, 7400, True, True, "Loja Oficial Samsung"),
        ("Notebook Samsung Galaxy Book Go Snapdragon 7c 4GB 128GB UFS Copilot", "Notebooks", 1899.00, 2299.00, "Samsung", 4.7, 9800, True, True, "Loja Oficial Samsung"),
        ("Notebook Samsung Galaxy Book4 Intel Core i5-1335U 8GB 512GB SSD Iris Xe", "Notebooks", 3499.00, 3999.00, "Samsung", 4.9, 6800, True, True, "Loja Oficial Samsung"),
        ("Notebook Acer Aspire 5 A515-45 AMD Ryzen 5 8GB 512GB SSD 15.6 Full HD", "Notebooks", 2999.00, 3499.00, "Acer", 4.8, 14200, True, True, "Loja Oficial Acer"),
        ("Notebook Dell Dc15-c3100-a20 15.6 FHD Core 3 8GB 512GB Windows 11", "Notebooks", 2899.00, 3399.00, "Dell", 4.8, 5600, True, True, "Loja Oficial Dell"),
        ("MacBook Neo 13 polegadas Chip A18 Pro CPU 6 núcleos 256GB Índigo", "Notebooks", 6999.00, 7999.00, "Apple", 4.9, 1200, True, True, "Loja Oficial Apple"),
        ("SSD Kingston NV2 1TB M.2 2280 NVMe PCIe 4.0 Até 3500MB/s", "Armazenamento", 389.00, 459.00, "Kingston", 4.9, 45200, True, True, "Loja Oficial Kingston"),
        ("SSD Kingston A400 480GB SATA III 2.5 Leituras 500MB/s", "Armazenamento", 219.00, 269.00, "Kingston", 4.9, 68500, True, True, "Loja Oficial Kingston"),
        ("SSD Kingston NV2 2TB M.2 2280 NVMe PCIe 4.0 Até 3500MB/s", "Armazenamento", 699.00, 799.00, "Kingston", 4.9, 18900, True, True, "Loja Oficial Kingston"),
        ("Mouse Gamer Sem Fio Logitech G305 Lightspeed 12000 DPI Preto", "Periféricos", 199.90, 249.00, "Logitech", 4.9, 32100, True, True, "Loja Oficial Logitech"),
        ("Mouse Sem Fio Ergonômico Logitech Lift Vertical Bluetooth Grafite", "Periféricos", 349.00, 429.00, "Logitech", 4.8, 9800, True, True, "Loja Oficial Logitech"),
        ("Teclado Mecânico Gamer Redragon Kumara RGB Switch Outemu Blue", "Periféricos", 199.90, 259.00, "Redragon", 4.8, 28700, True, True, "Marketplace Oficial"),
        ("Headset Gamer Redragon Zeus X RGB 7.1 Surround Som Imersivo", "Áudio PC", 249.90, 319.00, "Redragon", 4.8, 21400, True, True, "Marketplace Oficial"),
        ("Monitor Gamer LG UltraGear 24 Full HD 144Hz 1ms IPS FreeSync", "Monitores", 799.00, 949.00, "LG", 4.9, 24600, True, True, "Loja Oficial LG"),
        ("Monitor Gamer LG UltraGear 27 IPS 144Hz 1ms HDR10 Full HD", "Monitores", 999.00, 1199.00, "LG", 4.9, 18200, True, True, "Loja Oficial LG"),
        ("Memória RAM Kingston Fury Beast 8GB 3200MHz DDR4 CL16 Preto", "Hardware", 139.90, 179.00, "Kingston", 4.9, 39400, True, True, "Loja Oficial Kingston"),
        ("Memória RAM Kingston Fury Beast 16GB 3200MHz DDR4 CL16 Preto", "Hardware", 259.90, 319.00, "Kingston", 4.9, 27600, True, True, "Loja Oficial Kingston"),
        ("Processador AMD Ryzen 5 5600 6-Core 12-Threads 4.4GHz Cache 35MB", "Hardware", 789.00, 899.00, "AMD", 4.9, 19800, True, True, "Loja Oficial AMD"),
        ("Placa de Vídeo Galax GeForce RTX 4060 1-Click OC 2X 8GB GDDR6", "Hardware", 1899.00, 2199.00, "Galax", 4.9, 8700, True, True, "Loja Oficial Galax"),
        ("Fonte Corsair CV550 550W 80 Plus Bronze PFC Ativo Bivolt", "Hardware", 289.00, 349.00, "Corsair", 4.8, 16300, True, True, "Loja Oficial Corsair"),
        ("Gabinete Gamer Rise Mode Glass 06 Lateral e Frontal em Vidro", "Hardware", 199.90, 249.00, "Rise Mode", 4.7, 14200, True, True, "Marketplace Oficial"),
        ("Roteador Wi-Fi 6 TP-Link Archer AX12 Dual Band Gigabit 1500Mbps", "Redes", 229.00, 289.00, "TP-Link", 4.8, 18500, True, True, "Loja Oficial TP-Link"),
        ("Repetidor de Sinal Wi-Fi TP-Link RE200 AC750 Dual Band", "Redes", 129.90, 159.90, "TP-Link", 4.7, 34200, True, True, "Loja Oficial TP-Link"),
        ("Mousepad Gamer Extra Grande Speed 90x40cm Borda Costurada Preto", "Acessórios", 39.90, 59.90, "Exbom", 4.8, 42100, True, True, "Marketplace Oficial"),
        ("Pen Drive SanDisk Ultra Dual Drive USB Tipo C 128GB Flash", "Armazenamento", 79.90, 99.90, "SanDisk", 4.9, 31500, True, True, "Loja Oficial SanDisk"),
        ("Pen Drive SanDisk Cruzer Blade 64GB USB 2.0 Preto/Vermelho", "Armazenamento", 29.90, 45.00, "SanDisk", 4.8, 58400, True, True, "Loja Oficial SanDisk"),
        ("Mouse Sem Fio Logitech M170 Conexão USB Pilha Inclusa Cinza", "Periféricos", 59.90, 79.90, "Logitech", 4.8, 49200, True, True, "Loja Oficial Logitech"),
        ("Teclado Sem Fio Logitech K380 Bluetooth Multi-Device Cinza", "Periféricos", 189.90, 239.00, "Logitech", 4.9, 18700, True, True, "Loja Oficial Logitech"),
        ("Webcam Full HD 1080p Com Microfone Integrado Redragon Fobos", "Periféricos", 139.90, 179.90, "Redragon", 4.7, 12400, True, True, "Marketplace Oficial"),
        ("Suporte Articulado Para Monitor 17 a 35 Pistão a Gás F80N ELG", "Acessórios", 199.90, 259.00, "ELG", 4.9, 21800, True, True, "Loja Oficial ELG"),
        ("Placa Mãe Asus Prime B550M-A AMD AM4 DDR4 Micro ATX", "Hardware", 689.00, 799.00, "Asus", 4.8, 7400, True, True, "Loja Oficial Asus"),
        ("Water Cooler Rise Mode RGB 240mm Preto Bomba Cerâmica", "Hardware", 259.90, 319.00, "Rise Mode", 4.7, 9200, True, True, "Marketplace Oficial"),
        ("Pasta Térmica Arctic MX-4 4g Alta Condutividade Térmica", "Hardware", 49.90, 69.90, "Arctic", 4.9, 19800, True, False, "Marketplace Oficial"),
        ("Switch Gigabit TP-Link 8 Portas TL-SG108 Metal Desktop", "Redes", 149.90, 189.90, "TP-Link", 4.9, 11200, True, True, "Loja Oficial TP-Link"),
        ("Filtro de Linha DPS Clamper Energia 5 Tomadas Bivolt Preto", "Energia", 69.90, 89.90, "Clamper", 4.9, 38900, True, True, "Loja Oficial Clamper"),
        ("Cabo de Rede Cat6 UTP 10 Metros Conectores RJ45 Blindado", "Redes", 29.90, 45.00, "Furukawa", 4.8, 26700, True, False, "Marketplace Oficial"),
        ("Adaptador Bluetooth 5.0 USB Nano Para PC e Notebook Baseus", "Acessórios", 39.90, 55.00, "Baseus", 4.7, 31200, True, False, "Marketplace Oficial"),
        ("Impressora Multifuncional Tanque de Tinta Epson EcoTank L3250", "Impressão", 1099.00, 1299.00, "Epson", 4.8, 14200, True, True, "Loja Oficial Epson"),
        ("Kit 4 Garrafas Tinta Epson T544 Original Preto Ciano Magenta", "Suprimentos", 199.90, 249.00, "Epson", 4.9, 28100, True, True, "Loja Oficial Epson"),
        ("Controle Sem Fio Xbox Series Robot White Com Fio USB-C", "Games", 429.00, 499.00, "Microsoft", 4.9, 16800, True, True, "Loja Oficial Microsoft"),
        ("Controle Sem Fio DualSense PS5 PlayStation Branco Original", "Games", 419.00, 489.00, "Sony", 4.9, 19400, True, True, "Loja Oficial Sony"),
        ("Console PlayStation 5 Slim Digital Edição 1TB Astro Bot", "Games", 3699.00, 3999.00, "Sony", 4.9, 8900, True, True, "Loja Oficial Sony"),
        ("Console Nintendo Switch OLED 64GB Joy-Con Branco Bivolt", "Games", 2199.00, 2499.00, "Nintendo", 4.9, 11200, True, True, "Loja Oficial Nintendo"),
        ("Cadeira Gamer Reclinável Giratória Ergonômica ThunderX3 TGC12", "Móveis PC", 999.00, 1299.00, "ThunderX3", 4.8, 7600, True, True, "Marketplace Oficial"),
        ("Filamento Pla Branco Velvet Aveludado 1kg Impressoras 3D", "Impressão 3D", 99.90, 129.90, "Voolt3D", 4.9, 12400, True, True, "Loja Oficial Voolt3D"),
        ("Filamento Petg 3D Masterprint Cores Variadas 1kg", "Impressão 3D", 89.90, 119.90, "Masterprint", 4.8, 9800, True, True, "Marketplace Oficial")
    ],
    "Eletrodomésticos": [
        ("Electrolux Filtro de Água Refil Acqua Pure para Purificador PE12", "Purificadores", 99.90, 129.90, "Electrolux", 4.9, 45200, True, True, "Loja Oficial Electrolux"),
        ("Chaleira Elétrica Unitermi Elétrica Atacama Inox 1.8L Cinza", "Cozinha", 79.90, 99.90, "Unitermi", 4.8, 28900, True, True, "Marketplace Oficial"),
        ("Liquidificador Turbo Power Mondial 550W L-99 FR Preto", "Cozinha", 129.90, 169.90, "Mondial", 4.8, 102400, True, True, "Loja Oficial Mondial"),
        ("Ventilador De Mesa Super Power Vsp-30-b 30cm 6 Pás Preto", "Climatização", 139.90, 179.90, "Mondial", 4.8, 48500, True, True, "Loja Oficial Mondial"),
        ("Ventilador De Coluna Turbo Com 6 Pás 50cm Preto e Azul Ventisol", "Climatização", 189.90, 239.90, "Ventisol", 4.8, 31200, True, True, "Loja Oficial Ventisol"),
        ("Batedeira Prática Mondial 400W 3 Velocidades B-44-W Branca", "Cozinha", 119.90, 149.90, "Mondial", 4.8, 38700, True, True, "Loja Oficial Mondial"),
        ("Espremedor Premium Mondial 30W Bivolt Jarra 1.25L E-02", "Cozinha", 89.90, 119.90, "Mondial", 4.8, 42100, True, True, "Loja Oficial Mondial"),
        ("Grill Sanduicheira Cadence SAN400 Elétrica Chapa Misteira Click", "Cozinha", 99.90, 129.90, "Cadence", 4.9, 258000, True, True, "Loja Oficial Cadence"),
        ("Refil Filtro Cix06ax Purificador de Água Consul CPC30AF", "Purificadores", 89.90, 119.90, "Consul", 4.9, 34200, True, True, "Loja Oficial Consul"),
        ("Mixer Vertical Turbo Chef Elgin 3 Em 1 200W Preto", "Cozinha", 119.90, 159.90, "Elgin", 4.8, 104000, True, True, "Loja Oficial Elgin"),
        ("Fritadeira Sem Óleo Air Fryer Mondial AFN-40-BI 4 Litros Inox", "Cozinha", 319.90, 399.00, "Mondial", 4.8, 62400, True, True, "Loja Oficial Mondial"),
        ("Fritadeira Air Fryer Philco Dual Zone 8 Litros Cesto Duplo", "Cozinha", 699.00, 849.00, "Philco", 4.8, 18900, True, True, "Loja Oficial Philco"),
        ("Aspirador de Pó Vertical 2 em 1 Mondial Turbo Cycle 1100W", "Limpeza", 169.90, 219.00, "Mondial", 4.8, 67800, True, True, "Loja Oficial Mondial"),
        ("Fritadeira Air Fryer Philco Gourmet Black 4.4L Antiaderente", "Cozinha", 349.00, 429.00, "Philco", 4.7, 33200, True, True, "Loja Oficial Philco"),
        ("Aspirador Robô WAP Robot W100 Varre Aspira e Passa Pano", "Limpeza", 399.90, 499.00, "WAP", 4.6, 23900, True, True, "Loja Oficial WAP"),
        ("Micro-ondas Electrolux 31 Litros Painel Integrado Prata MI41S", "Cozinha", 689.00, 799.00, "Electrolux", 4.8, 16900, True, True, "Loja Oficial Electrolux"),
        ("Liquidificador Oster 1400 Full 3.2 Litros 15 Velocidades", "Cozinha", 179.90, 229.00, "Oster", 4.7, 28400, True, True, "Loja Oficial Oster"),
        ("Cafeteira Expresso Nescafé Dolce Gusto Mini Me Automática Arno", "Cozinha", 389.00, 479.00, "Arno", 4.9, 35100, True, True, "Loja Oficial Arno"),
        ("Aspirador Robô WAP Robot Wconnect Mapeamento Laser e Wi-Fi", "Limpeza", 1399.00, 1699.00, "WAP", 4.8, 9200, True, True, "Loja Oficial WAP"),
        ("Fritadeira Air Fryer Oster Touch 4.2 Litros Inox Digital", "Cozinha", 449.00, 549.00, "Oster", 4.8, 24100, True, True, "Loja Oficial Oster"),
        ("Ventilador de Coluna Mondial Turbo 8 Pás 40cm Ajustável Preto", "Climatização", 219.90, 269.00, "Mondial", 4.8, 28300, True, True, "Loja Oficial Mondial"),
        ("Cafeteira Expresso Três Corações Lov Automática Cápsulas Vermelha", "Cozinha", 399.00, 489.00, "Três Corações", 4.8, 22600, True, True, "Loja Oficial Três"),
        ("Ferro de Passar a Vapor Ceramic Gliss Arno Antiaderente Azul", "Cuidados Roupas", 119.90, 149.90, "Arno", 4.7, 23100, True, True, "Loja Oficial Arno"),
        ("Batedeira Planetária Mondial Black Premium 700W 4.5L Inox", "Cozinha", 299.90, 369.00, "Mondial", 4.7, 15600, True, True, "Loja Oficial Mondial"),
        ("Panela de Pressão Elétrica Electrolux 6L Display Digital Prata", "Cozinha", 489.00, 599.00, "Electrolux", 4.9, 20100, True, True, "Loja Oficial Electrolux"),
        ("Sanduicheira e Grill Britânia BVT30 Inox 750W Antiaderente", "Cozinha", 89.90, 119.90, "Britânia", 4.7, 39800, True, True, "Loja Oficial Britânia"),
        ("Umidificador de Ar Ultrassônico Bivolt Silencioso 3L Fisher Price", "Climatização", 129.90, 169.90, "Fisher Price", 4.8, 21400, True, True, "Marketplace Oficial"),
        ("Bebedouro Eletrônico Coluna Esmaltec Água Gelada e Natural Inox", "Bebedouros", 449.00, 549.00, "Esmaltec", 4.6, 10100, True, True, "Loja Oficial Esmaltec"),
        ("Cooktop 5 Bocas Vidro Temperado Preto Acendimento Automático", "Cozinha", 399.00, 489.00, "Itatiaia", 4.7, 17900, True, True, "Loja Oficial Itatiaia"),
        ("Forno Elétrico de Bancada Mueller Fratello 44 Litros Preto", "Cozinha", 459.00, 549.00, "Mueller", 4.8, 14800, True, True, "Loja Oficial Mueller"),
        ("Geladeira Brastemp Frost Free Duplex 375 Litros Inox BRM45HK", "Refrigeração", 3199.00, 3699.00, "Brastemp", 4.8, 8900, True, True, "Loja Oficial Brastemp"),
        ("Geladeira Consul Frost Free Duplex 340 Litros Branca CRD37", "Refrigeração", 2499.00, 2899.00, "Consul", 4.7, 11200, True, True, "Loja Oficial Consul"),
        ("Máquina de Lavar Electrolux 14kg Essencial Care Branca LED14", "Lavanderia", 1899.00, 2299.00, "Electrolux", 4.8, 14600, True, True, "Loja Oficial Electrolux"),
        ("Lava e Seca Midea 11kg HealthGuard Titanium Conectada Wi-Fi", "Lavanderia", 3499.00, 3999.00, "Midea", 4.9, 7800, True, True, "Loja Oficial Midea"),
        ("Climatizador de Ar Mondial Fresh Air 3.2L Frio Branco CL-03", "Climatização", 299.90, 379.00, "Mondial", 4.6, 16400, True, True, "Loja Oficial Mondial"),
        ("Purificador de Água Electrolux Pure 4X Eficiência Bacteriológica", "Purificadores", 549.00, 649.00, "Electrolux", 4.8, 22400, True, True, "Loja Oficial Electrolux"),
        ("Adegas Climatizadas 12 Garrafas Painel Digital Touch Philco", "Cozinha", 799.00, 949.00, "Philco", 4.7, 5400, True, True, "Loja Oficial Philco"),
        ("Cafeteira Elétrica Mondial Dolce Arome 18 Xícaras Inox C-30-18X", "Cozinha", 99.90, 139.90, "Mondial", 4.7, 34200, True, True, "Loja Oficial Mondial"),
        ("Processador de Alimentos Philips Walita Viva 750W 4 Acessórios", "Cozinha", 249.90, 319.00, "Philips Walita", 4.8, 18900, True, True, "Loja Oficial Philips"),
        ("Panela Elétrica de Arroz Mondial Bianca Rice 5 Xícaras Branca", "Cozinha", 139.90, 179.90, "Mondial", 4.8, 29800, True, True, "Loja Oficial Mondial"),
        ("Vaporizador de Roupas Portátil Black+Decker Passador a Vapor", "Cuidados Roupas", 149.90, 199.90, "Black+Decker", 4.6, 21800, True, True, "Loja Oficial Black"),
        ("Churrasqueira Elétrica Mondial Grand Steak Grill 2000W Inox", "Cozinha", 149.90, 189.90, "Mondial", 4.7, 31400, True, True, "Loja Oficial Mondial"),
        ("Centrífuga de Roupas Mueller Dry 8.8kg Roupa Seca Branca", "Lavanderia", 489.00, 589.00, "Mueller", 4.8, 8700, True, True, "Loja Oficial Mueller"),
        ("Aspirador de Pó e Água WAP GTW 10 Litros 1400W Amarelo/Preto", "Limpeza", 249.90, 319.00, "WAP", 4.8, 38200, True, True, "Loja Oficial WAP"),
        ("Depurador de Ar Slim 60cm Inox Retrátil 3 Velocidades Suggar", "Cozinha", 399.00, 489.00, "Suggar", 4.7, 12600, True, True, "Loja Oficial Suggar"),
        ("Fritadeira Sem Óleo Electrolux 3.2L Digital Painel Touch EAF20", "Cozinha", 389.00, 469.00, "Electrolux", 4.8, 19400, True, True, "Loja Oficial Electrolux"),
        ("Torradeira Elétrica Mondial T-06 Duo Inox 6 Níveis Tostagem", "Cozinha", 89.90, 119.90, "Mondial", 4.7, 21200, True, True, "Loja Oficial Mondial"),
        ("Ventilador de Teto com Controle Remoto Ventisol Fênix Branco", "Climatização", 279.00, 349.00, "Ventisol", 4.7, 14500, True, True, "Loja Oficial Ventisol"),
        ("Fatiador de Frios Elétrico Pratic Lâmina Aço Inox Lenoxx", "Cozinha", 299.90, 379.00, "Lenoxx", 4.6, 6800, True, True, "Marketplace Oficial"),
        ("Máquina de Costura Portátil Singer M1605 6 Pontos Bivolt", "Artesanato", 699.00, 829.00, "Singer", 4.8, 9200, True, True, "Loja Oficial Singer")
    ],
    "Casa, Móveis e Decoração": [
        ("Chuveiro Ducha Lorenzetti Loren Shower Eletrônico 6800W 220V", "Banheiro", 139.90, 179.90, "Lorenzetti", 4.9, 38400, True, True, "Loja Oficial Lorenzetti"),
        ("Jogo Lençol Casal 3pç 400 Fios Hipercal Conforto Macio Liso", "Cama e Banho", 89.90, 119.90, "Enxoval.com", 4.8, 29800, True, True, "Marketplace Oficial"),
        ("Faqueiro Tramontina 24 Peças Inox Jogo De Talheres Completo", "Cozinha", 99.90, 139.90, "Tramontina", 4.9, 65400, True, True, "Loja Oficial Tramontina"),
        ("Capa Protetora Colchão Box Casal Padrão Matelado Impermeável", "Cama e Banho", 69.90, 99.90, "Casa Laura", 4.8, 31200, True, True, "Marketplace Oficial"),
        ("Mangueira De Jardim 10m Trançada Reforçada Anti Dobra", "Jardim", 49.90, 69.90, "Marqs Home", 4.8, 24100, True, True, "Marketplace Oficial"),
        ("Tapete Carpete 200x140 Luizatex Peludo Felpudo 40mm Luxo", "Decoração", 119.90, 169.90, "Luizatex", 4.8, 18900, True, True, "Marketplace Oficial"),
        ("Câmera de Segurança Wi-Fi iCSee 4K Dupla Lente Prova D'água", "Segurança", 149.90, 199.90, "Woosh", 4.8, 21500, True, True, "Marketplace Oficial"),
        ("Cortina 2,00 x 1,80 Blackout Corta Luz Em Tecido Grosso", "Decoração", 79.90, 109.90, "Texfine", 4.8, 34200, True, True, "Marketplace Oficial"),
        ("Cadeira de Escritório Presidente Mesh Giratória Ergonômica Preto", "Móveis", 489.00, 599.00, "MobiStore", 4.8, 28400, True, True, "Marketplace Oficial"),
        ("Mesa Gamer Madesa 1.20m Com Nicho Suporte CPU Preto", "Móveis", 289.00, 369.00, "Madesa", 4.8, 19800, True, True, "Loja Oficial Madesa"),
        ("Cadeira de Escritório Diretor Giratória Mesh Apoio Braços Preto", "Móveis", 299.90, 379.00, "Conforsit", 4.7, 34200, True, True, "Marketplace Oficial"),
        ("Lâmpada Inteligente Smart Wi-Fi 10W RGB Positivo Bivolt", "Iluminação", 39.90, 59.90, "Positivo", 4.8, 51200, True, True, "Loja Oficial Positivo"),
        ("Travesseiro Nasa Alto Anatômico Viscoelástico Fibrasca 50x70", "Cama e Banho", 59.90, 79.90, "Fibrasca", 4.8, 43200, True, True, "Loja Oficial Fibrasca"),
        ("Kit 6 Potes Herméticos de Vidro Tampa Bambu Redondo Oikos", "Utilidades", 119.90, 159.90, "Oikos", 4.9, 18700, True, True, "Loja Oficial Oikos"),
        ("Jogo de Panelas Cerâmica Antiaderente 5 Peças Roma Brinox", "Cozinha", 399.00, 489.00, "Brinox", 4.8, 24100, True, True, "Loja Oficial Brinox"),
        ("Jogo de Panelas Tramontina Turim 7 Peças Antiaderente Preto", "Cozinha", 199.90, 259.00, "Tramontina", 4.8, 62100, True, True, "Loja Oficial Tramontina"),
        ("Chuveiro Lorenzetti Acqua Duo Ultra Eletrônico Preto/Cromado", "Banheiro", 389.00, 479.00, "Lorenzetti", 4.8, 31400, True, True, "Loja Oficial Lorenzetti"),
        ("Mop Giratório Fit Com Balde e Refil Microfibra FlashLimp", "Limpeza", 79.90, 109.90, "FlashLimp", 4.9, 78400, True, True, "Loja Oficial FlashLimp"),
        ("Armário Multiuso 2 Portas com Prateleiras Quarto Lavanderia", "Móveis", 219.00, 279.00, "Notável", 4.6, 21800, True, True, "Marketplace Oficial"),
        ("Guarda-Roupa Casal 6 Portas 2 Gavetas Lisboa Araplac", "Móveis", 499.00, 629.00, "Araplac", 4.6, 14200, True, True, "Loja Oficial Araplac"),
        ("Sapateira Organizadora Multiuso 4 Prateleiras Desmontável", "Organização", 39.90, 59.90, "Paramount", 4.7, 33200, True, True, "Marketplace Oficial"),
        ("Colchão Casal Espuma D33 Pró-Nascença Ortobom 138x188x17", "Cama e Banho", 689.00, 849.00, "Ortobom", 4.8, 16900, True, True, "Loja Oficial Ortobom"),
        ("Colchão Solteiro Espuma D28 Light Selado Ortobom 88x188x14", "Cama e Banho", 389.00, 469.00, "Ortobom", 4.8, 22100, True, True, "Loja Oficial Ortobom"),
        ("Lixeira Inox com Pedal e Balde Removível 5 Litros Tramontina", "Utilidades", 79.90, 109.90, "Tramontina", 4.9, 29400, True, True, "Loja Oficial Tramontina"),
        ("Kit 10 Cabides de Veludo Ultrafinos Antideslizante Preto", "Organização", 34.90, 49.90, "Buba", 4.8, 48700, True, True, "Marketplace Oficial"),
        ("Kit 4 Almofadas Decorativas Cheias 45x45 Geométricas Modernas", "Decoração", 69.90, 99.90, "DcorHome", 4.7, 24600, True, True, "Marketplace Oficial"),
        ("Espelho Redondo Adnet 60cm Alça Couro Pendurador Decorativo", "Decoração", 99.90, 139.90, "Premier", 4.8, 19400, True, True, "Marketplace Oficial"),
        ("Porta Temperos Giratório Inox com 16 Potes Vidro Bancada", "Cozinha", 109.90, 149.90, "Wellmix", 4.8, 18200, True, True, "Marketplace Oficial"),
        ("Varal de Chão com Abas Slim Aço Reforçado Dobrável Mor", "Lavanderia", 89.90, 119.90, "Mor", 4.8, 41200, True, True, "Loja Oficial Mor"),
        ("Kit 10 Panos de Prato Atoalhado Algodão Alta Absorção Dohler", "Cozinha", 49.90, 69.90, "Dohler", 4.8, 38700, True, True, "Loja Oficial Dohler"),
        ("Jogo de Toalhas Banho 4 Peças Canelada 100% Algodão Buddemeyer", "Cama e Banho", 149.90, 199.90, "Buddemeyer", 4.9, 17800, True, True, "Loja Oficial Buddemeyer"),
        ("Jogo de Cama Queen 4 Peças 200 Fios Algodão Percal Santista", "Cama e Banho", 139.90, 179.90, "Santista", 4.8, 21400, True, True, "Loja Oficial Santista"),
        ("Edredom Casal Dupla Face Soft Sherpa Toque de Pele Quente", "Cama e Banho", 129.90, 169.90, "Kacyumara", 4.8, 15600, True, True, "Marketplace Oficial"),
        ("Torneira Banheiro Misturador Monocomando Bica Baixa Cromada", "Banheiro", 119.90, 159.90, "Pingoo", 4.8, 26800, True, True, "Marketplace Oficial"),
        ("Papeleira de Chão Suporte Papel Higiênico Inox Banheiro", "Banheiro", 49.90, 69.90, "Future", 4.8, 31400, True, True, "Loja Oficial Future"),
        ("Escorredor de Louças 2 Andares Inox 16 Pratos com Porta Copos", "Cozinha", 89.90, 119.90, "Mak-Inox", 4.7, 22600, True, True, "Marketplace Oficial"),
        ("Kit 3 Prateleiras U Nicho Livros Quarto Infantil 50x10 MDF", "Móveis", 49.90, 69.90, "ArtFactory", 4.7, 19400, True, True, "Marketplace Oficial"),
        ("Abajur de Mesa Luminária Articulada Base Pesada Escritório", "Iluminação", 69.90, 89.90, "G-Light", 4.8, 28700, True, True, "Marketplace Oficial"),
        ("Fita de LED RGB 5050 5 Metros com Controle e Fonte Bivolt", "Iluminação", 39.90, 59.90, "Importado", 4.6, 64200, True, True, "Marketplace Oficial"),
        ("Quadro Decorativo Mosaico 5 Peças Leão Colorido Sala Quarto", "Decoração", 79.90, 109.90, "ArteQuadros", 4.8, 18900, True, True, "Marketplace Oficial"),
        ("Relógio de Parede Silencioso Moderno 30cm Sala Cozinha", "Decoração", 49.90, 69.90, "Herweg", 4.8, 23400, True, True, "Loja Oficial Herweg"),
        ("Mala de Bordo Viagem Rígida Rodas 360 Graus Padrão ANAC P", "Malas", 189.90, 249.00, "Santino", 4.8, 34200, True, True, "Loja Oficial Santino"),
        ("Organizador de Gavetas Colmeia Kit 6 Peças Calcinhas Sutiã", "Organização", 39.90, 55.00, "VbHome", 4.8, 29800, True, True, "Marketplace Oficial"),
        ("Dispenser Detergente Dosador Sabão Líquido Embutir Pia Inox", "Cozinha", 39.90, 59.90, "Tramontina", 4.9, 32100, True, True, "Loja Oficial Tramontina"),
        ("Cesto Roupas Sujas Dobrável Tecido Impermeável 60 Litros", "Lavanderia", 44.90, 65.00, "Oikos", 4.7, 26400, True, True, "Loja Oficial Oikos"),
        ("Assento Sanitário Almofadado Universal Branco Durável Astra", "Banheiro", 49.90, 69.90, "Astra", 4.8, 48200, True, True, "Loja Oficial Astra"),
        ("Fechadura Digital Biométrica Wi-Fi Inteligente Intelbras FR 101", "Segurança", 429.00, 529.00, "Intelbras", 4.9, 16400, True, True, "Loja Oficial Intelbras"),
        ("Aparador Buffet 3 Portas Retrô Sala de Estar Pequeno Madesa", "Móveis", 289.00, 369.00, "Madesa", 4.7, 14200, True, True, "Loja Oficial Madesa"),
        ("Kit 2 Travesseiros Suporte Firme Altenburg Sono Perfeito", "Cama e Banho", 79.90, 109.90, "Altenburg", 4.8, 31200, True, True, "Loja Oficial Altenburg"),
        ("Puff Baú Organizador Multiuso Suede Sala Quarto 70cm", "Móveis", 129.90, 169.90, "SuedeArt", 4.7, 18900, True, True, "Marketplace Oficial")
    ],
    "Ferramentas e Construção": [
        ("Chuveiro Ducha Lorenzetti Loren Shower Eletrônico 6800W 220V", "Elétrica", 139.90, 179.90, "Lorenzetti", 4.9, 38400, True, True, "Loja Oficial Lorenzetti"),
        ("Régua Extensão Elétrica 6 Tomadas Filtro Linha 10A 2 Metros", "Elétrica", 39.90, 59.90, "Parosflux", 4.8, 29800, True, True, "Marketplace Oficial"),
        ("Torneira Gourmet Luxo Flexível Cozinha Parede 2 Jatos Aço Inox", "Hidráulica", 89.90, 129.90, "Camperluz", 4.8, 34200, True, True, "Marketplace Oficial"),
        ("Manta Asfáltica Terracota Autoadesiva 45cm X 10m Vedatudo Dryko", "Construção", 99.90, 139.90, "Dryko", 4.9, 21400, True, True, "Loja Oficial Dryko"),
        ("Spray Vedatudo Vazamentos De Água Reparo Telha Laje Dryko", "Construção", 49.90, 69.90, "Dryko", 4.8, 18900, True, True, "Loja Oficial Dryko"),
        ("Kit 2 Adaptadores universal para tomada viagem Bivolt", "Elétrica", 29.90, 45.00, "Parosflux", 4.8, 41200, True, True, "Marketplace Oficial"),
        ("Filtro de Linha DPS iClamper Energia 5 Tomadas Anti Raio Preto", "Elétrica", 79.90, 99.90, "Clamper", 4.9, 48200, True, True, "Loja Oficial Clamper"),
        ("Extensão Elétrica 5 Metros Preto 3 Tomadas Tripolar", "Elétrica", 34.90, 49.90, "Parosflux", 4.8, 26800, True, True, "Marketplace Oficial"),
        ("Adaptador Tomada 20A P/ 10A Carregador Carro Elétrico Bivolt", "Elétrica", 29.90, 45.00, "LEF", 4.8, 19400, True, True, "Marketplace Oficial"),
        ("Lorenzetti Chuveiro Elétrico Advanced Multitemperaturas 127V", "Elétrica", 129.90, 159.90, "Lorenzetti", 4.8, 45600, True, True, "Loja Oficial Lorenzetti"),
        ("Furadeira de Impacto Bosch GSB 13 RE 750W 1/2 Pol Mandril Chave", "Elétricas", 349.00, 429.00, "Bosch", 4.9, 38400, True, True, "Loja Oficial Bosch"),
        ("Parafusadeira Furadeira Mondial 12V Bateria Íon-Lítio FPF-05", "Elétricas", 149.90, 199.90, "Mondial", 4.8, 54200, True, True, "Loja Oficial Mondial"),
        ("Parafusadeira e Furadeira de Impacto DeWalt 20V Max DCD7781", "Elétricas", 899.00, 1099.00, "DeWalt", 4.9, 18900, True, True, "Loja Oficial DeWalt"),
        ("Parafusadeira e Furadeira Makita HP333DZ 12V CXT Sem Bateria", "Elétricas", 389.00, 469.00, "Makita", 4.9, 14200, True, True, "Loja Oficial Makita"),
        ("Maleta de Ferramentas Tramontina 129 Peças Aço com Alicates", "Manuais", 189.90, 249.00, "Tramontina", 4.8, 42100, True, True, "Loja Oficial Tramontina"),
        ("Jogo de Chaves Fenda e Philips 6 Peças Imantadas Tramontina", "Manuais", 39.90, 59.90, "Tramontina", 4.8, 51400, True, True, "Loja Oficial Tramontina"),
        ("Serra Mármore Makita 4100NH3ZX2 1450W 12000 RPM com 2 Discos", "Elétricas", 399.00, 489.00, "Makita", 4.9, 29800, True, True, "Loja Oficial Makita"),
        ("Esmerilhadeira Angular Bosch GWS 700 710W 4.1/2 Pol 127V", "Elétricas", 299.00, 369.00, "Bosch", 4.9, 26400, True, True, "Loja Oficial Bosch"),
        ("Trena a Laser Digital Mileseey 40 Metros Display Iluminado", "Medição", 119.90, 169.90, "Mileseey", 4.8, 23100, True, True, "Marketplace Oficial"),
        ("Lavadora de Alta Pressão Kärcher K2 Plus 1400W 1740 PSI", "Limpeza", 449.00, 549.00, "Kärcher", 4.8, 21800, True, True, "Loja Oficial Kärcher"),
        ("Lavadora de Alta Pressão WAP Ousada Plus 2200 1500W 1750 PSI", "Limpeza", 429.00, 519.00, "WAP", 4.8, 28700, True, True, "Loja Oficial WAP"),
        ("Inversora de Solda Boxer Flama 161 BV Bivolt Automática 160A", "Solda", 899.00, 1099.00, "Boxer", 4.9, 9400, True, True, "Loja Oficial Boxer"),
        ("Máscara de Solda Automática Escurecimento Solar Regulável", "Segurança", 79.90, 119.90, "Titanium", 4.8, 31200, True, True, "Marketplace Oficial"),
        ("Alicate Universal 8 Polegadas Isolado 1000V Tramontina Pro", "Manuais", 44.90, 65.00, "Tramontina", 4.9, 49800, True, True, "Loja Oficial Tramontina"),
        ("Alicate de Pressão Mordente Curvo 10 Polegadas Vonder", "Manuais", 49.90, 69.90, "Vonder", 4.8, 34200, True, True, "Loja Oficial Vonder"),
        ("Nível de Alumínio 3 Bolhas Magnético 40cm Sparta", "Medição", 29.90, 45.00, "Sparta", 4.7, 28900, True, True, "Marketplace Oficial"),
        ("Jogo de Chaves Combinadas 6 a 22mm 12 Peças Aço Cromo Vonder", "Manuais", 119.90, 159.90, "Vonder", 4.8, 24100, True, True, "Loja Oficial Vonder"),
        ("Kit Brocas Aço Rápido Titânio para Metal Madeira Concreto 16pçs", "Acessórios", 49.90, 69.90, "Bosch", 4.8, 41200, True, True, "Loja Oficial Bosch"),
        ("Disco de Corte Fino Inox 4.1/2 Pol Caixa 10 Peças Norton", "Acessórios", 39.90, 55.00, "Norton", 4.9, 56400, True, True, "Loja Oficial Norton"),
        ("Compressor de Ar Direto Chiaperini Ar Direto 1/2HP Bivolt", "Pneumática", 799.00, 949.00, "Chiaperini", 4.7, 6800, True, True, "Loja Oficial Chiaperini"),
        ("Pistola de Pintura Gravidade Bico 1.4mm HVLP 600ml Steula", "Pintura", 129.90, 169.90, "Steula", 4.8, 14200, True, True, "Marketplace Oficial"),
        ("Escada de Alumínio Articulada 4x3 12 Degraus Multifuncional Mor", "Construção", 399.00, 499.00, "Mor", 4.8, 18900, True, True, "Loja Oficial Mor"),
        ("Serrote Profissional 20 Polegadas Dente Temperado Tramontina", "Manuais", 39.90, 55.00, "Tramontina", 4.8, 22400, True, True, "Loja Oficial Tramontina"),
        ("Trena Métrica Profissional 5 Metros com Fita de Aço Lufkin", "Medição", 34.90, 49.90, "Lufkin", 4.9, 45100, True, True, "Loja Oficial Lufkin"),
        ("Kit Chaves Allen Sextavadas 1.5 a 10mm 9 Peças Curtas Gedore", "Manuais", 39.90, 55.00, "Gedore", 4.9, 32100, True, True, "Loja Oficial Gedore"),
        ("Fita Isolante 3M Imperial 20 Metros Anti Chamas Preta", "Elétrica", 9.90, 15.00, "3M", 4.9, 98400, True, True, "Loja Oficial 3M"),
        ("Multímetro Digital Portátil com Bip Sonoro e Pontas Prova", "Elétrica", 39.90, 59.90, "Minipa", 4.7, 37200, True, True, "Marketplace Oficial"),
        ("Pistola de Cola Quente Profissional 40W Bivolt Tramontina", "Acessórios", 39.90, 55.00, "Tramontina", 4.8, 31400, True, True, "Loja Oficial Tramontina"),
        ("Refletor LED 100W Holofote Prova D'água IP66 Branco Frio Bivolt", "Iluminação", 49.90, 69.90, "Avant", 4.7, 42100, True, True, "Loja Oficial Avant"),
        ("Sensor de Presença Infravermelho Teto Parede 360 Graus Margirius", "Elétrica", 39.90, 55.00, "Margirius", 4.8, 24600, True, True, "Marketplace Oficial"),
        ("Fita Crepe Automotiva e Pintura Imobiliária 48mm x 50m 3M", "Pintura", 19.90, 28.00, "3M", 4.9, 48200, True, True, "Loja Oficial 3M"),
        ("Rolo de Lã para Pintura Anti Gota 23cm com Garfo Atlas", "Pintura", 29.90, 42.00, "Atlas", 4.8, 33200, True, True, "Loja Oficial Atlas"),
        ("Espátula de Aço Inox Cabo Madeira 10cm Tramontina", "Pintura", 19.90, 28.00, "Tramontina", 4.8, 21400, True, True, "Loja Oficial Tramontina"),
        ("Silicone Neutro Multiuso Transparente 280g Tubo Tekbond", "Construção", 24.90, 35.00, "Tekbond", 4.8, 62100, True, True, "Loja Oficial Tekbond"),
        ("Veda Calha Alumínio Adesivo Selante Poliuretano 400g Pulvitec", "Construção", 29.90, 42.00, "Pulvitec", 4.8, 28400, True, True, "Loja Oficial Pulvitec"),
        ("Chave Inglesa Ajustável 10 Polegadas Aço Carbono Vonder", "Manuais", 49.90, 69.90, "Vonder", 4.8, 24600, True, True, "Loja Oficial Vonder"),
        ("Martelo Unha Polido 27mm Cabo Madeira Envernizado Tramontina", "Manuais", 39.90, 55.00, "Tramontina", 4.9, 51200, True, True, "Loja Oficial Tramontina"),
        ("Grampeador Tapeceiro Manual Aço 4 a 14mm com 1000 Grampos", "Manuais", 49.90, 69.90, "Sparta", 4.7, 31200, True, True, "Marketplace Oficial"),
        ("Soprador Térmico Profissional 2000W 2 Temperaturas Gamma", "Elétricas", 149.90, 199.90, "Gamma", 4.8, 18900, True, True, "Loja Oficial Gamma"),
        ("Broca Escalonada HSS Titânio Cônica 4 a 32mm para Chapas Metal", "Acessórios", 39.90, 59.90, "Groove", 4.7, 26400, True, True, "Marketplace Oficial")
    ]
}

# Verificação das 5 categorias
for cat, prods in CATALOGO_250.items():
    print(f"✅ {cat}: {len(prods)} produtos cadastrados.")

def identificar_rodada(dt_brt):
    hora = dt_brt.hour
    dia_semana = dt_brt.weekday()
    eh_sabado = (dia_semana == 5)
    
    if hora < 13:
        rotulo = "08:00 - Abertura Sabado (Varredura Semanal Delta)" if eh_sabado else "08:00 - Abertura Manha"
        return {"rotulo": rotulo, "fator_volume": 0.40 if eh_sabado else 0.35, "descricao": "Abertura matinal"}
    elif hora < 21:
        return {"rotulo": "18:00 - Pico Tarde", "fator_volume": 0.85 if eh_sabado else 0.80, "descricao": "Pico de trafego comercial"}
    else:
        return {"rotulo": "23:00 - Fechamento Noturno", "fator_volume": 1.00, "descricao": "Fechamento consolidado do dia"}

def extrair_lote_diario(dt_brt=None):
    if dt_brt is None:
        dt_brt = datetime.now(FUSO_BRT)
        
    hoje = dt_brt.date()
    ano = hoje.year
    mes = hoje.month
    ano_mes = hoje.strftime("%Y-%m")
    is_weekend = hoje.weekday() >= 5
    
    info_rodada = identificar_rodada(dt_brt)
    logger.info(f"Executando Rodada: [{info_rodada['rotulo']}] - {info_rodada['descricao']}.")
    
    rows = []
    
    for cat_nome, prods_50 in CATALOGO_250.items():
        seed_dia = int(hoje.strftime("%Y%m%d")) + abs(hash(cat_nome)) + dt_brt.hour
        rng = np.random.default_rng(abs(seed_dia) % (2**32 - 1))
        
        scores = []
        for rank_base, prod_item in enumerate(prods_50, start=1):
            ruido = rng.normal(0, 1.2)
            score = (55 - rank_base) + ruido
            scores.append((score, prod_item, rank_base))
            
        scores.sort(key=lambda x: x[0], reverse=True)
        
        for rank_atual, (_, prod_item, rank_original) in enumerate(scores, start=1):
            t_nome, subcat, p_base, p_orig_ref, marca, nota, avaliacoes, is_full, frete, loja = prod_item
            
            prob_promo = 0.28 if is_weekend else 0.16
            tem_promo = rng.random() < prob_promo
            fator_promo = rng.choice([0.88, 0.92, 0.95]) if tem_promo else 1.0
            
            preco_atual_val = round(p_base * fator_promo, 2)
            preco_orig_val = round(p_orig_ref, 2) if tem_promo else round(p_base, 2)
            desconto_pct_val = int(round(((preco_orig_val - preco_atual_val) / preco_orig_val) * 100)) if tem_promo else 0
            
            if "Moto G04" in t_nome:
                preco_atual_val = 699.00
                preco_orig_val = 1399.00
                desconto_pct_val = 50
                nota = 4.9
                avaliacoes = 31769
                loja = "Loja Oficial Motorola"
                
            parcelamento = "em 10x sem juros" if preco_atual_val > 300 else ("em 6x sem juros" if preco_atual_val > 100 else "em 3x sem juros")
            
            base_v = 720 / (rank_atual ** 0.65)
            multiplicador_dia = 1.25 if is_weekend else 1.0
            fator_horario = info_rodada["fator_volume"]
            
            vendas_dia_total = int(max(10, base_v * multiplicador_dia * fator_horario * rng.uniform(0.92, 1.10)))
            faturamento_dia_total = round(vendas_dia_total * preco_atual_val, 2)
            
            mlb_id = f"MLB-{1000000000 + (abs(hash(t_nome)) % 900000000)}"
            rep = "MercadoLíder Platinum" if is_full else "MercadoLíder Gold"
            img = f"https://http2.mlstatic.com/D_NQ_NP_{abs(hash(mlb_id)) % 999999}-MLA-O.webp"
            prod_url = f"https://produto.mercadolivre.com.br/{mlb_id}"
            
            rows.append({
                "data": hoje,
                "ano": int(ano),
                "mes": int(mes),
                "ano_mes": str(ano_mes),
                "posicao_ranking": int(rank_atual),
                "categoria": str(cat_nome),
                "subcategoria": str(subcat),
                "id_anuncio": str(mlb_id),
                "titulo_produto": str(t_nome),
                "marca": str(marca),
                "preco_atual": str(preco_atual_val).replace(".", ","),
                "preco_original": str(preco_orig_val).replace(".", ","),
                "desconto_pct": str(desconto_pct_val),
                "parcelamento": str(parcelamento),
                "qtd_vendas_estimadas_dia": int(vendas_dia_total),
                "faturamento_estimado_dia": str(faturamento_dia_total).replace(".", ","),
                "avaliacao_nota": str(round(float(nota), 1)).replace(".", ","),
                "qtd_avaliacoes": int(avaliacoes + int(rng.integers(5, 30))),
                "is_full": bool(is_full),
                "frete_gratis": bool(frete),
                "loja_oficial": str(loja),
                "reputacao_vendedor": str(rep),
                "url_imagem": str(img),
                "url_produto": str(prod_url),
                "tipo_dado": "Sintético"
            })
            
    df = pd.DataFrame(rows)
    df["desconto_pct"] = df["desconto_pct"].astype(str)
    df["is_full"] = df["is_full"].astype(bool)
    df["frete_gratis"] = df["frete_gratis"].astype(bool)
    logger.info(f"Lote diario montado com {len(df)} registros para {hoje.strftime('%d/%m/%Y')} ({info_rodada['rotulo']}).")
    return df

def carregar_bigquery(df, client):
    tabela_completa = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABELA_ID}"
    hoje_str = df["data"].iloc[0].strftime("%Y-%m-%d")
    
    logger.info(f"Limpando dados anteriores de {hoje_str} no BigQuery via CTAS...")
    try:
        ctas_query = f"""
        CREATE OR REPLACE TABLE `{tabela_completa}` AS
        SELECT * FROM `{tabela_completa}` WHERE data != '{hoje_str}'
        """
        client.query(ctas_query).result()
    except Exception as e:
        logger.warning(f"Aviso na limpeza previa via CTAS: {e}")
        
    df_bq = df.copy()
    df_bq["desconto_pct"] = df_bq["desconto_pct"].astype(str)
    df_bq["is_full"] = df_bq["is_full"].astype(bool)
    df_bq["frete_gratis"] = df_bq["frete_gratis"].astype(bool)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_dataframe(df_bq, tabela_completa, job_config=job_config)
    job.result()
    logger.info(f"[OK] {len(df)} linhas gravadas no BigQuery com sucesso.")

def sincronizar_arquivos_locais(df_dia):
    hoje_date = df_dia["data"].iloc[0]
    
    if os.path.exists(LOCAL_PARQUET):
        try:
            df_hist = pd.read_parquet(LOCAL_PARQUET)
            df_hist = df_hist[df_hist["data"] != hoje_date]
            df_dia_pq = df_dia.copy()
            cols_to_use = [c for c in df_hist.columns if c in df_dia_pq.columns]
            df_dia_pq = df_dia_pq[cols_to_use]
            for col in cols_to_use:
                try:
                    df_dia_pq[col] = df_dia_pq[col].astype(df_hist[col].dtype)
                except Exception:
                    pass
            df_completo = pd.concat([df_hist, df_dia_pq], ignore_index=True)
            df_completo.to_parquet(LOCAL_PARQUET, index=False)
            logger.info(f"[OK] Parquet local atualizado ({len(df_completo)} linhas).")
        except Exception as e:
            logger.warning(f"Aviso no Parquet: {e}")
            
    if os.path.exists(LOCAL_CSV):
        try:
            df_hist_csv = pd.read_csv(LOCAL_CSV, sep=";", encoding="utf-8-sig")
            hoje_br = hoje_date.strftime("%d/%m/%Y")
            df_hist_csv = df_hist_csv[df_hist_csv["data"] != hoje_br]
            df_dia_csv = df_dia.copy()
            df_dia_csv["data"] = hoje_br
            cols_csv = [c for c in df_hist_csv.columns if c in df_dia_csv.columns]
            df_dia_csv = df_dia_csv[cols_csv]
            df_completo_csv = pd.concat([df_hist_csv, df_dia_csv], ignore_index=True)
            df_completo_csv.to_csv(LOCAL_CSV, sep=";", index=False, encoding="utf-8-sig")
            logger.info(f"[OK] CSV local atualizado ({len(df_completo_csv)} linhas).")
        except Exception as e:
            logger.warning(f"Aviso no CSV: {e}")

if __name__ == "__main__":
    agora_brt = datetime.now(FUSO_BRT)
    info = identificar_rodada(agora_brt)
    logger.info(f"Iniciando rotina Mercado Livre: [{info['rotulo']}]")
    
    df_lote = extrair_lote_diario(agora_brt)
    sincronizar_arquivos_locais(df_lote)
    
    bq_client = obter_cliente_bigquery()
    if bq_client:
        carregar_bigquery(df_lote, bq_client)
    else:
        logger.warning("BigQuery indisponivel. Dados salvos apenas localmente.")
