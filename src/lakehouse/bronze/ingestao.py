# Databricks notebook source
# MAGIC %md
# MAGIC # Processo de ingestão dos arquivos repousados no ADLS para camada bronze. 
# MAGIC
# MAGIC ##### Nesse processo é realizado a leitura de arquivos (Estruturados, Semi-estruturados ou Não estruturados) criando External Table no formato Delta.

# COMMAND ----------

import sys
import os
from datetime import datetime

# -------- # 
sys.path.insert(0, "../../lib/") # Inserindo o diretório para a primeira posição da lista

# Importação dos módulos
import utils
import ingestors

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Recebendo parâmetros do Job

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("database", "")
dbutils.widgets.text("table_name", "")

# ----------------------------------------------- #

dbutils.widgets.dropdown("file_format", "csv", ["csv", "json", "text"], "file_format")
dbutils.widgets.text("delimiter", "")
dbutils.widgets.dropdown("multiline", "False", ["True", "False"], "multiline")
dbutils.widgets.dropdown("header", "True", ["True", "False"], "header")

# ----------------------------------------------- #

dbutils.widgets.text("account_name", "adlsprj001")
dbutils.widgets.text("container_name", "raw")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database =  dbutils.widgets.get("database")
table_name = dbutils.widgets.get("table_name") 

# ----------------------------------------------- #

file_format = dbutils.widgets.get("file_format")
delimiter = dbutils.widgets.get("delimiter")
multiline = dbutils.widgets.get("multiline")
header = dbutils.widgets.get("header")

# ----------------------------------------------- #

BLOB_ACCOUNT_NAME = dbutils.widgets.get("account_name")
BLOB_CONTAINER_NAME = dbutils.widgets.get("container_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexão com o ADLS para leitura dos arquvios via Service Principal (Azure Active Directory)

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

# Diretório de origem do arquivo & diretório de destino
ADLS_PATH = f"abfss://{BLOB_CONTAINER_NAME}@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/{file_format}/"
EXTERNAL_PATH = f"abfss://bronze@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/ingestao/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa de instanciação das classes concretas seguindo o Design Pattern Factory.

# COMMAND ----------

# Conversão do schema para o StructType
schema = utils.load_schema("./schema.json")

# Instancia da classe *IngestorFactory*
ingestor = ingestors.IngestorFactory.get_ingestor(file_format, spark, catalog, database, table_name, schema, delimiter, multiline, header)

# Invocação do método *ingest* herdado da interface
ingestor.ingest(ADLS_PATH, EXTERNAL_PATH, partitions=["gender"])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databricks_ws_datamaster.bronze.tb_bronze
