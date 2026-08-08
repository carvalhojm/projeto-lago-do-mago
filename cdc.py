# %%

"""
cdc.py

Simula uma extração incremental (CDC) em cima do dataset "TeoMeWhy Loyalty System".

Roda para cada tabela listada em FILES (mesma lista do fullload.py). Como não
existe um binlog real disponível para nós, este script usa o fullload de cada
tabela (gerado por fullload.py) como "banco de dados fonte" e, a cada execução,
libera um novo lote de registros como se fossem alterações capturadas do banco:

    - INSERT: próximos registros ainda não emitidos (novos pedidos "chegando")
    - UPDATE: amostra de registros já emitidos, com um valor numérico alterado
    - DELETE: amostra de registros já emitidos, marcados como removidos

O estado (quais IDs já foram "emitidos") fica salvo em data/_state/cdc_state_<tabela>.json,
um arquivo por tabela, então cada execução avança o cursor de cada uma -- rode o
script várias vezes (manualmente no início, depois com um agendador a cada 10min)
para simular o fluxo do curso.

Uso:
    python cdc.py
"""

import json
import os
import random
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME", "")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY", "")

DATASET = "teocalvo/teomewhy-loyalty-system"

# mesma lista usada no fullload.py -- mantenha os dois arquivos sincronizados
FILES = [
    "transacoes.csv",
    "transacao_produto.csv",
    "clientes.csv",
]

BASE_DIR = Path(os.getenv("LANDING_DIR", "./data"))
RAW_DIR = BASE_DIR / "_raw"
FULLLOAD_DIR = BASE_DIR / "fullload"
CDC_DIR = BASE_DIR / "cdc"
STATE_DIR = BASE_DIR / "_state"

CDC_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = int(os.getenv("CDC_BATCH_SIZE", 500))       # novos INSERTs por rodada
UPDATE_RATE = float(os.getenv("CDC_UPDATE_RATE", 0.03))  # % dos já emitidos que sofrem UPDATE
DELETE_RATE = float(os.getenv("CDC_DELETE_RATE", 0.01))  # % dos já emitidos que sofrem DELETE


# ---------------------------------------------------------------------------
# Fonte dos dados
# ---------------------------------------------------------------------------
def load_source_dataframe(table_name: str, file_name: str) -> pd.DataFrame:
    """Usa o fullload local da tabela se existir; senão baixa o CSV do Kaggle."""
    latest_fullload = FULLLOAD_DIR / table_name / f"{table_name}_fullload_latest.parquet"
 
    if latest_fullload.exists():
        print(f"[cdc] Usando fullload existente: {latest_fullload}")
        return pd.read_parquet(latest_fullload)
 
    print(f"[cdc] Nenhum fullload de {table_name} encontrado, baixando do Kaggle...")
    from kaggle.api.kaggle_api_extended import KaggleApi
 
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(DATASET, file_name=file_name, path=str(RAW_DIR), force=True)
 
    zip_path = RAW_DIR / f"{file_name}.zip"
    csv_path = RAW_DIR / file_name
    if zip_path.exists():
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(RAW_DIR)
        zip_path.unlink()
 
    return pd.read_csv(csv_path, sep=";")
 
 
def detect_id_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        if "id" in col.lower():
            return col
    df["_row_id"] = df.index  # fallback: id sintético
    return "_row_id"
 
 
def detect_date_column(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        lowered = col.lower()
        if "data" in lowered or lowered.startswith("dt"):
            try:
                pd.to_datetime(df[col])
                return col
            except Exception:
                continue
    return None
 
 
# ---------------------------------------------------------------------------
# Estado do CDC
# ---------------------------------------------------------------------------
def load_state(table_name: str) -> dict:
    state_path = STATE_DIR / f"cdc_state_{table_name}.json"
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {"emitted_ids": [], "run_count": 0}
 
 
def save_state(table_name: str, state: dict):
    state_path = STATE_DIR / f"cdc_state_{table_name}.json"
    state_path.write_text(json.dumps(state, default=str))
 
 
# ---------------------------------------------------------------------------
# Núcleo do CDC (roda para uma tabela por vez)
# ---------------------------------------------------------------------------
def run_for_table(file_name: str):
    table_name = Path(file_name).stem  # ex: "transacoes", "clientes", "produtos"
    print(f"\n[cdc] === Tabela: {table_name} ===")
 
    df = load_source_dataframe(table_name, file_name)
    id_col = detect_id_column(df)
    date_col = detect_date_column(df)
 
    if date_col:
        df = df.sort_values(date_col)
    print(f"[cdc] Coluna de id: {id_col} | coluna de data: {date_col or 'não detectada (usando ordem original)'}")
 
    state = load_state(table_name)
    previous_emitted_ids = set(state["emitted_ids"])
 
    df["_id_str"] = df[id_col].astype(str)
    pending = df[~df["_id_str"].isin(previous_emitted_ids)]
    already_emitted_df = df[df["_id_str"].isin(previous_emitted_ids)]
 
    changes = []
 
    # --- INSERTs: próximo lote de registros "novos" ---
    inserts = pending.head(BATCH_SIZE).copy()
    newly_emitted_ids = set(inserts["_id_str"]) if not inserts.empty else set()
    if not inserts.empty:
        inserts["_operation"] = "INSERT"
        changes.append(inserts)
 
    # --- UPDATEs: amostra de registros já emitidos ---
    if not already_emitted_df.empty and UPDATE_RATE > 0:
        n_updates = max(1, int(len(already_emitted_df) * UPDATE_RATE))
        updates = already_emitted_df.sample(min(n_updates, len(already_emitted_df))).copy()
        numeric_cols = [c for c in updates.select_dtypes(include="number").columns if c != id_col]
        if numeric_cols:
            target_col = numeric_cols[0]
            updates[target_col] = updates[target_col] * random.uniform(0.9, 1.1)
        updates["_operation"] = "UPDATE"
        changes.append(updates)
 
    # --- DELETEs: amostra de registros já emitidos, marcados como removidos ---
    if not already_emitted_df.empty and DELETE_RATE > 0:
        n_deletes = max(1, int(len(already_emitted_df) * DELETE_RATE))
        deletes = already_emitted_df.sample(min(n_deletes, len(already_emitted_df))).copy()
        deletes["_operation"] = "DELETE"
        changes.append(deletes)
 
    if not changes:
        print(f"[cdc] {table_name}: nada novo para emitir (todos os registros já foram processados).")
        return
 
    cdc_batch = pd.concat(changes, ignore_index=True).drop(columns=["_id_str"])
    extracted_at = datetime.utcnow()
    cdc_batch["_extracted_at"] = extracted_at.isoformat()
    cdc_batch["_load_type"] = "cdc"
 
    partition = extracted_at.strftime("%Y%m%d_%H%M%S")
    table_cdc_dir = CDC_DIR / table_name
    table_cdc_dir.mkdir(parents=True, exist_ok=True)
    out_path = table_cdc_dir / f"{table_name}_cdc_{partition}.parquet"
    cdc_batch.to_parquet(out_path, index=False)
 
    print(
        f"[cdc] {len(cdc_batch)} linhas emitidas "
        f"({(cdc_batch['_operation'] == 'INSERT').sum()} INSERT, "
        f"{(cdc_batch['_operation'] == 'UPDATE').sum()} UPDATE, "
        f"{(cdc_batch['_operation'] == 'DELETE').sum()} DELETE)"
    )
    print(f"[cdc] Parquet salvo em: {out_path}")
 
    state["emitted_ids"] = list(previous_emitted_ids | newly_emitted_ids)
    state["run_count"] = state.get("run_count", 0) + 1
    save_state(table_name, state)
 
 
def run():
    for file_name in FILES:
        run_for_table(file_name)
 
 
if __name__ == "__main__":
    run()