"""
fullload.py

Simula uma carga FULL LOAD do dataset "TeoMeWhy Loyalty System" (Kaggle).
Baixa transacoes.csv via API do Kaggle e "pousa" o resultado como um único
arquivo .parquet, imitando o snapshot completo que no curso vai para o S3.

Pré-requisitos:
    pip install -r requirements.txt
    Preencher o .env com KAGGLE_USERNAME e KAGGLE_KEY

Uso:
    python fullload.py
"""
#%%

import os
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
# %%
load_dotenv()
 
# a lib do kaggle lê credenciais das env vars KAGGLE_USERNAME / KAGGLE_KEY
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME", "")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY", "")
 
DATASET = "teocalvo/teomewhy-loyalty-system"
 
# nomes exatos como aparecem em "Data Explorer" na página do dataset no Kaggle
FILES = [
    "transacoes.csv",
    "transacao_produto.csv", 
    "clientes.csv", 
]
 
BASE_DIR = Path(os.getenv("LANDING_DIR", "./data"))
RAW_DIR = BASE_DIR / "_raw"
FULLLOAD_DIR = BASE_DIR / "fullload"
 
RAW_DIR.mkdir(parents=True, exist_ok=True)
FULLLOAD_DIR.mkdir(parents=True, exist_ok=True)
 
 
def get_api():
    from kaggle.api.kaggle_api_extended import KaggleApi
 
    api = KaggleApi()
    api.authenticate()
    return api
 
 
def download_csv(api, file_name: str) -> Path:
    """Baixa um arquivo do dataset para RAW_DIR, descompactando se preciso."""
    print(f"[fullload] Baixando {file_name} de {DATASET}...")
    api.dataset_download_file(DATASET, file_name=file_name, path=str(RAW_DIR), force=True)
 
    zip_path = RAW_DIR / f"{file_name}.zip"
    csv_path = RAW_DIR / file_name
 
    # caso a API do Kaggle zipe arquivo único
    if zip_path.exists():
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(RAW_DIR)
        zip_path.unlink()
 
    if not csv_path.exists():
        raise FileNotFoundError(f"Não encontrei {csv_path} depois do download.")
 
    return csv_path
 
 
def load_one_table(api, file_name: str, extracted_at: datetime, partition: str):
    csv_path = download_csv(api, file_name)
 
    df = pd.read_csv(csv_path, sep=";")
    print(f"[fullload] {len(df):,} linhas lidas de {csv_path.name}")
 
    df["_extracted_at"] = extracted_at.isoformat()
    df["_load_type"] = "fullload"
 
    table_name = csv_path.stem  # ex: "transacoes", "clientes", "produtos"
    table_dir = FULLLOAD_DIR / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
 
    out_path = table_dir / f"{table_name}_fullload_{partition}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[fullload] Parquet salvo em: {out_path}")
 
    # mantém uma cópia "latest" por tabela -> o cdc.py usa ela como fonte de verdade
    latest_path = table_dir / f"{table_name}_fullload_latest.parquet"
    df.to_parquet(latest_path, index=False)
    print(f"[fullload] Atualizado: {latest_path}")
 
 
def run():
    api = get_api()
    extracted_at = datetime.utcnow()
    partition = extracted_at.strftime("%Y%m%d_%H%M%S")
 
    for file_name in FILES:
        load_one_table(api, file_name, extracted_at, partition)
 
 
if __name__ == "__main__":
    run()