# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1622395c-921b-46c4-a47d-213b8afb5f3f",
# META       "default_lakehouse_name": "Lakehouse_Bronze",
# META       "default_lakehouse_workspace_id": "071769de-561e-408b-9e24-ffb7a5db965f",
# META       "known_lakehouses": [
# META         {
# META           "id": "1622395c-921b-46c4-a47d-213b8afb5f3f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================================
# NOTEBOOK: Notebook_Ingest_Orders
# TASK: High-volume data ingestion
#
# RESPONSABILIDADE:
# - Baixar o ZIP da origem
# - Extrair todos os CSVs
# - Armazenar os arquivos na Bronze
# - Não realizar transformações
# =====================================================================================

import os
import requests
import zipfile
import tempfile
from datetime import datetime

# =====================================================================================
# CONFIGURAÇÕES
# =====================================================================================

SOURCE_URL = "https://github.com/MicrosoftLearning/dp-data/raw/main/orders.zip"

BRONZE_PATH = "/lakehouse/default/Files/bronze"

os.makedirs(BRONZE_PATH, exist_ok=True)

print(f"Destino Bronze: {BRONZE_PATH}")

# =====================================================================================
# DOWNLOAD
# =====================================================================================

print("Iniciando download...")

response = requests.get(SOURCE_URL)

if response.status_code != 200:
    raise Exception(
        f"Falha ao baixar arquivo. Status Code: {response.status_code}"
    )

# =====================================================================================
# ARQUIVO TEMPORÁRIO
# =====================================================================================

temp_zip = tempfile.NamedTemporaryFile(
    suffix=".zip",
    delete=False
)

temp_zip.write(response.content)
temp_zip.close()

print(f"ZIP temporário criado: {temp_zip.name}")

# =====================================================================================
# EXTRAÇÃO
# =====================================================================================

files_loaded = []

print("Extraindo arquivos...")

with zipfile.ZipFile(temp_zip.name, "r") as zip_ref:

    for file_name in zip_ref.namelist():

        # Ignora diretórios
        if file_name.endswith("/"):
            continue

        # Processa apenas CSV
        if not file_name.lower().endswith(".csv"):
            continue

        destination_file = os.path.join(
            BRONZE_PATH,
            os.path.basename(file_name)
        )

        with zip_ref.open(file_name) as source:
            with open(destination_file, "wb") as target:
                target.write(source.read())

        files_loaded.append(
            os.path.basename(file_name)
        )

# =====================================================================================
# LIMPEZA
# =====================================================================================

os.remove(temp_zip.name)

print("Arquivo ZIP temporário removido.")

# =====================================================================================
# INVENTÁRIO
# =====================================================================================

print("\nArquivos carregados:")

for file in sorted(files_loaded):
    print(f" - {file}")

print(
    f"\nTotal de arquivos CSV carregados: {len(files_loaded)}"
)

# =====================================================================================
# AUDITORIA
# =====================================================================================

audit_info = {
    "execution_time": datetime.utcnow().isoformat(),
    "source": SOURCE_URL,
    "files_loaded": len(files_loaded)
}

print("\nResumo da execução:")
print(audit_info)

print("\nIngestão concluída com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
