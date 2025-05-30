import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import re
import unicodedata
from datetime import datetime
from sqlalchemy import create_engine

# === CONFIGURAÇÃO DO BANCO DE DADOS ===
db_user = "postgres"
db_password = "46824682"
db_host = "localhost"
db_port = "5432"
db_name = "Alper_Case"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# === MAPA DE MESES ===
month_variants = {
    "JANEIRO": 1, "FEVEREIRO": 2, "MARO": 3, "MARCO": 3, "MARÇO": 3, "MAR": 3,
    "ABRIL": 4, "MAIO": 5, "JUNHO": 6, "JUN": 6,
    "JULHO": 7, "JUL": 7, "AGOSTO": 8, "SETEMBRO": 9, "OUTUBRO": 10,
    "NOVEMBRO": 11, "DEZEMBRO": 12
}

def normalize_text(text):
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")
    return re.sub(r"[^A-Z0-9]", "", text.upper())

def extract_date_from_url(url):
    filename = url.split("/")[-1]
    normalized = normalize_text(filename)

    month = None
    for name, number in month_variants.items():
        if name in normalized:
            month = number
            break

    year_match = re.search(r"(20\d{2})", normalized)
    year = int(year_match.group(1)) if year_match else None

    if month and year:
        return datetime(year, month, 1)
    return None

def normalize_columns(columns):
    normalized = []
    for col in columns:
        col = str(col).strip()
        col = unicodedata.normalize("NFKD", col).encode("ASCII", "ignore").decode("utf-8")
        col = re.sub(r"[^a-zA-Z0-9]+", "_", col).strip("_").upper()
        normalized.append(col)
    return normalized

def get_links(base_url):
    print(f"[STEP] Fetching page: {base_url}")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        return soup.find_all("a", href=True)
    except Exception as e:
        print(f"[ERROR] Could not fetch page: {e}")
        return []

# === INÍCIO DO PROCESSAMENTO ===
dataframes = []
base_urls = [
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-2024",
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-2025"
]

for base_url in base_urls:
    links = get_links(base_url)

    for link in links:
        text = link.get_text(strip=True).lower()
        href = link["href"]
        if "frota por município" in text and href.lower().endswith((".xls", ".xlsx")):
            try:
                file_url = href if href.startswith("http") else "https://www.gov.br" + href
                print(f"[STEP] Downloading: {file_url}")

                file_resp = requests.get(file_url)
                file_resp.raise_for_status()
                file_content = io.BytesIO(file_resp.content)

                df = pd.read_excel(file_content, header=2, skiprows=[3])
                df.dropna(axis=1, how="all", inplace=True)
                df.columns = normalize_columns(df.columns)

                if "UF" not in df.columns or "MUNICIPIO" not in df.columns:
                    print(f"[WARNING] Skipping file with missing UF or MUNICIPIO: {file_url}")
                    continue

                df["DATA"] = extract_date_from_url(file_url)

                id_vars = ["UF", "MUNICIPIO", "DATA"]
                value_vars = [col for col in df.columns if col not in id_vars]
                df_melt = df.melt(id_vars=id_vars, value_vars=value_vars,
                                  var_name="TIPO_VEICULO", value_name="QUANTIDADE")

                dataframes.append(df_melt)
                print("[OK] Processed file.")

            except Exception as e:
                print(f"[ERROR] Failed to process file {file_url}: {e}")

# === INSERÇÃO NO POSTGRES ===
if dataframes:
    final_df = pd.concat(dataframes, ignore_index=True)
    try:
        final_df.to_sql("frota_municipio_tipo_veiculo", engine, if_exists="replace", index=False)
        print("[OK] Dados inseridos na tabela 'frota_municipio_tipo_veiculo' no PostgreSQL.")
    except Exception as e:
        print(f"[ERROR] Falha ao inserir no PostgreSQL: {e}")
else:
    print("[WARNING] Nenhum dado foi processado.")
