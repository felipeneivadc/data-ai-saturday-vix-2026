# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "801ecffe-29a4-436e-8329-0b0f54683c9c",
# META       "default_lakehouse_name": "Lakehouse_Gold",
# META       "default_lakehouse_workspace_id": "071769de-561e-408b-9e24-ffb7a5db965f",
# META       "known_lakehouses": [
# META         {
# META           "id": "801ecffe-29a4-436e-8329-0b0f54683c9c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🥇 Silver → Gold — Modelo Estrela (Star Schema) para Análise
# 
# **Camada de origem:** `Lakehouse_Silver` (tabela `dbo.silver_orders`)
# **Camada de destino:** `Lakehouse_Gold` (tabelas `dbo.dimdate_gold`, `dbo.dimcustomer_gold`,
# `dbo.dimproduct_gold`, `dbo.factsales_gold`)
# 
# Este notebook transforma a tabela `silver_orders` em um modelo dimensional
# (**Kimball star schema**), pronto para relatórios e para um modelo semântico do Power BI:
# 
# - `dimdate_gold` — dimensão de data (Day, Month, Year, mmmyyyy, yyyymm)
# - `dimcustomer_gold` — dimensão de cliente (chave substituta `CustomerID`)
# - `dimproduct_gold` — dimensão de produto (chave substituta `ProductID`)
# - `factsales_gold` — tabela fato, grão = 1 linha por item de pedido, com as chaves das dimensões e a métrica `SalesAmount`
# 
# Todas as tabelas são criadas/gravadas usando **nome de tabela do catálogo**
# (`saveAsTable` / `DeltaTable.forName`), para que sejam corretamente registradas no
# metastore da Lakehouse_Gold (evitando o problema de pastas **"Unidentified"** que
# ocorre ao gravar direto em um caminho ABFSS que não segue a convenção `Tables/<schema>/<tabela>`).


# CELL ********************

# =====================================================================================
# IMPORTS E RESOLUÇÃO DO CAMINHO DE LEITURA (OneLake)
# - LEITURA da Silver: via caminho ABFSS da tabela Delta (não precisa anexar a Lakehouse_Silver)
# - ESCRITA no Gold: via nome de tabela qualificado pelo catálogo ("dbo.<tabela>")
# =====================================================================================
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, dayofmonth, month, year, date_format, split, lit, when, trim,
    monotonically_increasing_id, coalesce, max as spark_max, round as spark_round
)
from delta.tables import DeltaTable
import notebookutils

silver_lh = notebookutils.lakehouse.getWithProperties("Lakehouse_Silver")
SILVER_TABLE_PATH = f'{silver_lh["properties"]["abfsPath"]}/Tables/dbo/silver_orders'

DIMDATE_TABLE = "dbo.dimdate_gold"
DIMCUSTOMER_TABLE = "dbo.dimcustomer_gold"
DIMPRODUCT_TABLE = "dbo.dimproduct_gold"
FACTSALES_TABLE = "dbo.factsales_gold"

# 1. Carrega a tabela Silver (fonte única de verdade para o Gold) via caminho ABFSS
sales_df = spark.read.format("delta").load(SILVER_TABLE_PATH)
print(f"Total de linhas lidas da silver_orders: {sales_df.count()}")
display(sales_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 2. DIMENSÃO DE DATA (dbo.dimdate_gold)
#    NOVO: coluna DateKey (Integer, formato yyyyMMdd) -> usada como Entity Type Key
#    simples na Ontology, no lugar da chave composta Year+Month+Day.
# =====================================================================================
dim_date_df = (
    sales_df.dropDuplicates(["OrderDate"])
    .select(
        col("OrderDate"),
        date_format(col("OrderDate"), "yyyyMMdd").cast("int").alias("DateKey"),
        dayofmonth("OrderDate").alias("Day"),
        month("OrderDate").alias("Month"),
        year("OrderDate").alias("Year"),
        date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"),
        date_format(col("OrderDate"), "yyyyMM").alias("yyyymm")
    )
)

if not spark.catalog.tableExists(DIMDATE_TABLE):
    dim_date_df.write.format("delta").mode("overwrite").saveAsTable(DIMDATE_TABLE)
else:
    # Garante que a coluna DateKey exista mesmo em tabelas criadas antes deste ajuste
    existing_cols = [c.lower() for c in spark.table(DIMDATE_TABLE).columns]
    if "datekey" not in existing_cols:
        spark.sql(f"ALTER TABLE {DIMDATE_TABLE} ADD COLUMNS (DateKey INT)")
        print(f"Coluna 'DateKey' adicionada a {DIMDATE_TABLE} (tabela ja existente).")

    (
        DeltaTable.forName(spark, DIMDATE_TABLE).alias("gold")
        .merge(dim_date_df.alias("updates"), "gold.OrderDate = updates.OrderDate")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"{DIMDATE_TABLE}: {spark.table(DIMDATE_TABLE).count()} linhas")
display(spark.table(DIMDATE_TABLE).orderBy(col("OrderDate").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 3. DIMENSÃO DE CLIENTE (dbo.dimcustomer_gold) — com chave substituta (surrogate key)
# =====================================================================================
customer_stage_df = (
    sales_df.dropDuplicates(["CustomerName", "Email"])
    .select("CustomerName", "Email")
    .withColumn("First", split(col("CustomerName"), " ").getItem(0))
    .withColumn("Last", split(col("CustomerName"), " ").getItem(1))
)

if not spark.catalog.tableExists(DIMCUSTOMER_TABLE):
    (
        customer_stage_df
        .withColumn("CustomerID", monotonically_increasing_id() + lit(1))
        .write.format("delta").mode("overwrite").saveAsTable(DIMCUSTOMER_TABLE)
    )
else:
    customer_existing_df = spark.table(DIMCUSTOMER_TABLE)
    max_customer_id = customer_existing_df.select(
        coalesce(spark_max(col("CustomerID")), lit(0)).alias("MaxID")
    ).first()[0]

    # Somente clientes que ainda não existem na dimensão recebem um novo CustomerID
    customer_new_df = (
        customer_stage_df.alias("stg")
        .join(
            customer_existing_df.alias("exist"),
            (col("stg.CustomerName") == col("exist.CustomerName")) & (col("stg.Email") == col("exist.Email")),
            "left_anti"
        )
        .withColumn("CustomerID", monotonically_increasing_id() + lit(max_customer_id) + 1)
    )

    (
        DeltaTable.forName(spark, DIMCUSTOMER_TABLE).alias("gold")
        .merge(
            customer_new_df.alias("updates"),
            "gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"{DIMCUSTOMER_TABLE}: {spark.table(DIMCUSTOMER_TABLE).count()} linhas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 4. DIMENSÃO DE PRODUTO (dbo.dimproduct_gold)
#    O campo "Item" vem como "Nome do produto, Tamanho" (ex.: "Mountain-100 Black, 42")
# =====================================================================================
product_stage_df = (
    sales_df.dropDuplicates(["Item"])
    .select("Item")
    .withColumn("ItemName", trim(split(col("Item"), ",").getItem(0)))
    .withColumn("ItemSize", trim(split(col("Item"), ",").getItem(1)))
)

if not spark.catalog.tableExists(DIMPRODUCT_TABLE):
    (
        product_stage_df
        .withColumn("ProductID", monotonically_increasing_id() + lit(1))
        .write.format("delta").mode("overwrite").saveAsTable(DIMPRODUCT_TABLE)
    )
else:
    product_existing_df = spark.table(DIMPRODUCT_TABLE)
    max_product_id = product_existing_df.select(
        coalesce(spark_max(col("ProductID")), lit(0)).alias("MaxID")
    ).first()[0]

    product_new_df = (
        product_stage_df.alias("stg")
        .join(product_existing_df.alias("exist"), col("stg.Item") == col("exist.Item"), "left_anti")
        .withColumn("ProductID", monotonically_increasing_id() + lit(max_product_id) + 1)
    )

    (
        DeltaTable.forName(spark, DIMPRODUCT_TABLE).alias("gold")
        .merge(product_new_df.alias("updates"), "gold.Item = updates.Item")
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"{DIMPRODUCT_TABLE}: {spark.table(DIMPRODUCT_TABLE).count()} linhas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 5. TABELA FATO DE VENDAS (dbo.factsales_gold)
#    Grão: 1 linha por item de pedido (SalesOrderNumber + SalesOrderLineNumber)
#    Junta com as dimensões para trazer as chaves substitutas (surrogate keys)
#    NOVO: inclui DateKey (Integer, yyyyMMdd) junto com OrderDate, para permitir o
#    binding do relacionamento Sales -> Date na Ontology usando uma unica chave inteira.
# =====================================================================================
dim_customer_df = spark.table(DIMCUSTOMER_TABLE)
dim_product_df = spark.table(DIMPRODUCT_TABLE)
dim_date_df_gold = spark.table(DIMDATE_TABLE)

fact_stage_df = (
    sales_df.alias("s")
    .join(
        dim_customer_df.alias("c"),
        (col("s.CustomerName") == col("c.CustomerName")) & (col("s.Email") == col("c.Email")),
        "left"
    )
    .join(dim_product_df.alias("p"), col("s.Item") == col("p.Item"), "left")
    .join(dim_date_df_gold.alias("d"), col("s.OrderDate") == col("d.OrderDate"), "left")
    .withColumn("SalesAmount", spark_round((col("s.Quantity") * col("s.UnitPrice")) + col("s.Tax"), 2))
    .select(
        col("s.SalesOrderNumber"),
        col("s.SalesOrderLineNumber"),
        col("c.CustomerID"),
        col("p.ProductID"),
        col("d.OrderDate"),
        col("d.DateKey"),
        col("s.Quantity"),
        col("s.UnitPrice"),
        col("s.Tax"),
        col("SalesAmount")
    )
)

if not spark.catalog.tableExists(FACTSALES_TABLE):
    fact_stage_df.write.format("delta").mode("overwrite").saveAsTable(FACTSALES_TABLE)
else:
    # Garante que a coluna DateKey exista mesmo em tabelas criadas antes deste ajuste
    existing_cols = [c.lower() for c in spark.table(FACTSALES_TABLE).columns]
    if "datekey" not in existing_cols:
        spark.sql(f"ALTER TABLE {FACTSALES_TABLE} ADD COLUMNS (DateKey INT)")
        print(f"Coluna 'DateKey' adicionada a {FACTSALES_TABLE} (tabela ja existente).")

    (
        DeltaTable.forName(spark, FACTSALES_TABLE).alias("gold")
        .merge(
            fact_stage_df.alias("updates"),
            "gold.SalesOrderNumber = updates.SalesOrderNumber "
            "AND gold.SalesOrderLineNumber = updates.SalesOrderLineNumber"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"{FACTSALES_TABLE}: {spark.table(FACTSALES_TABLE).count()} linhas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# 6. VALIDAÇÃO FINAL — visão consolidada do modelo estrela na Lakehouse_Gold
#    Confirma que DateKey esta preenchido corretamente nas duas tabelas
# =====================================================================================
df_fact_check = spark.table(FACTSALES_TABLE)
print(f"Total de linhas em {FACTSALES_TABLE}: {df_fact_check.count()}")
print(f"Linhas com DateKey nulo em {FACTSALES_TABLE}: {df_fact_check.filter(col('DateKey').isNull()).count()}")
display(df_fact_check.select("SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "DateKey", "CustomerID", "ProductID", "SalesAmount").orderBy(col("OrderDate").desc()).limit(10))

df_date_check = spark.table(DIMDATE_TABLE)
print(f"Total de linhas em {DIMDATE_TABLE}: {df_date_check.count()}")
print(f"DateKey distintos: {df_date_check.select('DateKey').distinct().count()} (deve ser igual ao total de linhas)")
display(df_date_check.orderBy(col("OrderDate").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
