# Databricks notebook source
# MAGIC %md
# MAGIC # Processo de tratamento de dados para camada Silver
# MAGIC ### Nesse processo é realizado o tratamento dos dados *Bronze* passando por um Pipeline criando uma External Table no formato Delta

# COMMAND ----------

import sys
import os

# -------- # 

sys.path.insert(0, "../../lib/") # Inserindo o diretório para a primeira posição da lista

# Importação dos módulos
import utils
import pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recebendo parâmetros do Job

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_ws_datamaster")

dbutils.widgets.text("database", "bronze")
dbutils.widgets.text("current_database", "silver")

dbutils.widgets.text("table_name", "tb_bronze_autoloader")
dbutils.widgets.text("current_table_name", "tb_silver")

# ----------------------------------------------- #

dbutils.widgets.text("account_name", "adlsprj001")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

database =  dbutils.widgets.get("database")
current_database =  dbutils.widgets.get("current_database")

table_name = dbutils.widgets.get("table_name") 
current_table_name =  dbutils.widgets.get("current_table_name")

# ----------------------------------------------- #

BLOB_ACCOUNT_NAME = dbutils.widgets.get("account_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexão com o ADLS via Service Principal (Azure Active Directory)

# COMMAND ----------

# Recuperando valores do SPN armazenados no Azure Key Vault (AKV)
CLIENT_ID = dbutils.secrets.get(scope="akv-scope", key="CLIENT-ID")
CLIENT_SECRET = dbutils.secrets.get(scope="akv-scope", key="CLIENTSECRET")
TENANT_ID = dbutils.secrets.get(scope="akv-scope", key="TENANTID")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{BLOB_ACCOUNT_NAME}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{BLOB_ACCOUNT_NAME}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{BLOB_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_ID)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{BLOB_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_SECRET)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{BLOB_ACCOUNT_NAME}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token")


# COMMAND ----------

# diretório de destino
EXTERNAL_PATH = f"abfss://silver@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura da tabela Bronze 

# COMMAND ----------

df_bronze = spark.sql("SELECT * FROM {}.{}.{}".format(catalog, database, table_name)) # Carregando dados da bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa de instância do Pipeline de ETL e criação da tabela Silver

# COMMAND ----------

config = utils.load_config("./contract.yaml")
steps = ["remove_duplicates", "convert_types", "null_handling"]

pipe = pipeline.DynamicPipe(df_bronze, steps, config) # Instância do Pipeline.
df_silver = pipe.execute() # Executando o Pipeline (steps -> Transformação dos dados).

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{EXTERNAL_PATH}/{current_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da External Table

# COMMAND ----------

utils.create_external_table(spark, catalog, current_database, current_table_name, f"{EXTERNAL_PATH}/{current_table_name}")

# COMMAND ----------

# %sql
# DROP TABLE databricks_ws_datamaster.silver.tb_silver
