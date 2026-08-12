# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ce736bf7-7215-4518-8754-3d10060593fc",
# META       "default_lakehouse_name": "Lakehouse_Silver",
# META       "default_lakehouse_workspace_id": "071769de-561e-408b-9e24-ffb7a5db965f",
# META       "known_lakehouses": [
# META         {
# META           "id": "ce736bf7-7215-4518-8754-3d10060593fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🥈 Bronze → Silver — Padronização e Limpeza dos Pedidos
# 
# **Camada de origem:** `Lakehouse_Bronze` (Files/bronze/*.csv)
# **Camada de destino:** `Lakehouse_Silver` (Tables/silver_orders)
# 
# Este notebook lê todos os arquivos CSV brutos da camada Bronze, aplica um schema
# explícito, executa regras de qualidade/enriquecimento e grava (via **merge/upsert**)
# a tabela Delta `silver_orders` na Lakehouse_Silver — evitando duplicar dados caso a
# Bronze seja reprocessada.

# CELL ********************

# =====================================================================================
# IMPORTS E RESOLUÇÃO DO CAMINHO DE LEITURA (OneLake)
# - LEITURA da Bronze: via caminho ABFSS (não precisa anexar a Lakehouse_Bronze).
# - ESCRITA na Silver: via nome de tabela qualificado pelo catálogo ("dbo.silver_orders"),
#   e não por caminho ABFSS bruto. Isso garante que a tabela seja corretamente
#   registrada no metastore da Lakehouse_Silver (Tables > dbo > silver_orders),
#   sem cair em "Unidentified".
# - Por isso a Lakehouse_Silver PRECISA estar anexada como Lakehouse padrão.
# =====================================================================================
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, FloatType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, when, lit, current_timestamp, input_file_name, trim, lower
from delta.tables import DeltaTable
import notebookutils

# Resolve apenas o caminho ABFSS da Lakehouse_Bronze (somente leitura, não precisa anexar)
bronze_lh = notebookutils.lakehouse.getWithProperties("Lakehouse_Bronze")
BRONZE_FILES_PATH = f'{bronze_lh["properties"]["abfsPath"]}/Files/bronze'

# Nome de tabela qualificado pelo catálogo (schema.tabela) na Lakehouse_Silver (anexada como padrão)
SILVER_TABLE_NAME = "dbo.silver_orders"

print(f"Lendo CSVs de     : {BRONZE_FILES_PATH}/*.csv")
print(f"Gravando tabela   : {SILVER_TABLE_NAME}  (na Lakehouse_Silver, anexada como padrão)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 1. SCHEMA EXPLÍCITO DOS PEDIDOS (evita inferência incorreta de tipos pelo Spark)
# =====================================================================================
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

# =====================================================================================
# 2. LEITURA DE TODOS OS ARQUIVOS CSV DISPONÍVEIS NA CAMADA BRONZE
#    O padrão "*.csv" cobre 2019.csv, 2020.csv, 2021.csv e qualquer novo arquivo futuro
# =====================================================================================
df_bronze = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(orderSchema)
    .load(f"{BRONZE_FILES_PATH}/*.csv")
)

total_bronze = df_bronze.count()
print(f"Total de linhas lidas da Bronze: {total_bronze}")
display(df_bronze.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 3. REGRAS DE QUALIDADE E ENRIQUECIMENTO (a Silver = dados limpos e conformados)
#    - FileName              : rastreabilidade do arquivo de origem
#    - IsFlagged              : marca pedidos anteriores a uma data de corte (regra de exemplo)
#    - CreatedTS / ModifiedTS : auditoria de carga
#    - CustomerName           : preenche nulos/vazios com "Unknown" e remove espaços extras
#    - Email                  : normaliza para minúsculo e remove espaços; nulos -> "unknown"
#    - dropDuplicates         : remove duplicidade exata por pedido + linha do pedido
# =====================================================================================
df_silver = (
    df_bronze
    .withColumn("FileName", input_file_name())
    .withColumn("IsFlagged", when(col("OrderDate") < "2019-08-01", True).otherwise(False))
    .withColumn("CreatedTS", current_timestamp())
    .withColumn("ModifiedTS", current_timestamp())
    .withColumn(
        "CustomerName",
        when((col("CustomerName").isNull()) | (trim(col("CustomerName")) == ""), lit("Unknown"))
        .otherwise(trim(col("CustomerName")))
    )
    .withColumn(
        "Email",
        when(col("Email").isNull(), lit("unknown@unknown.com"))
        .otherwise(lower(trim(col("Email"))))
    )
    .dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])
)

total_silver = df_silver.count()
print(f"Total apos limpeza/dedup: {total_silver} (removidas/afetadas: {total_bronze - total_silver})")
display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 4/5. CRIA (se necessário) e faz MERGE/UPSERT na tabela dbo.silver_orders
#    - Usa saveAsTable() na primeira execução (cria e REGISTRA a tabela no catálogo)
#    - Usa DeltaTable.forName() + merge nas execuções seguintes (idempotente)
#    Chave de negocio para o merge: SalesOrderNumber + SalesOrderLineNumber
# =====================================================================================
if not spark.catalog.tableExists(SILVER_TABLE_NAME):
    print(f"Tabela '{SILVER_TABLE_NAME}' não existe. Criando pela primeira vez...")
    df_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE_NAME)
else:
    print(f"Tabela '{SILVER_TABLE_NAME}' já existe. Executando merge/upsert...")
    silver_table = DeltaTable.forName(spark, SILVER_TABLE_NAME)
    (
        silver_table.alias("silver")
        .merge(
            df_silver.alias("updates"),
            "silver.SalesOrderNumber = updates.SalesOrderNumber "
            "AND silver.SalesOrderLineNumber = updates.SalesOrderLineNumber"
        )
        .whenMatchedUpdate(set={
            "OrderDate": "updates.OrderDate",
            "CustomerName": "updates.CustomerName",
            "Email": "updates.Email",
            "Item": "updates.Item",
            "Quantity": "updates.Quantity",
            "UnitPrice": "updates.UnitPrice",
            "Tax": "updates.Tax",
            "FileName": "updates.FileName",
            "IsFlagged": "updates.IsFlagged",
            "ModifiedTS": "updates.ModifiedTS"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"Tabela '{SILVER_TABLE_NAME}' atualizada com sucesso na Lakehouse_Silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 6. VALIDAÇÃO RÁPIDA
# =====================================================================================
df_check = spark.table(SILVER_TABLE_NAME)
print(f"Total de registros em {SILVER_TABLE_NAME}: {df_check.count()}")
display(df_check.orderBy(col("OrderDate").desc()).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
