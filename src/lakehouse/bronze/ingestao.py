# Databricks notebook source
# MAGIC %md
# MAGIC # Processo de ingestão dos arquivos repousados no ADLS para camada bronze. 
# MAGIC
# MAGIC ##### Nesse processo é realizado a leitura de arquivos (Estruturados, Semi-estruturados ou Não estruturados) criando External Table no formato Delta.

# COMMAND ----------

import sys
import os

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
CLIENT_ID = dbutils.secrets.get(scope="spn-secret-scope", key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope="spn-secret-scope", key="client-secret")
TENANT_ID = dbutils.secrets.get(scope="spn-secret-scope", key="tenant-id")

# COMMAND ----------

# Realizando conexão com o ADLS utilizando as credenciais do SPN
spark.conf.set(f"spark.hadoop.fs.azure.account.auth.type.{BLOB_ACCOUNT_NAME}.blob.core.windows.net", "OAuth")
spark.conf.set(f"spark.hadoop.fs.azure.account.oauth.provider.type.{BLOB_ACCOUNT_NAME}.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.id.{BLOB_ACCOUNT_NAME}.blob.core.windows.net", CLIENT_ID)
spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{BLOB_ACCOUNT_NAME}.blob.core.windows.net", CLIENT_SECRET)
spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{BLOB_ACCOUNT_NAME}.blob.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")


# COMMAND ----------

# Diretório de origem do arquivo & diretório de destino
ADLS_PATH = f"abfss://{BLOB_CONTAINER_NAME}@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/{BLOB_CONTAINER_NAME}/"
EXTERNAL_PATH = f"abfss://{BLOB_CONTAINER_NAME}@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/bronze/"
CHECKPOINT_PATH = f"abfss://{BLOB_CONTAINER_NAME}@{BLOB_ACCOUNT_NAME}.dfs.core.windows.net/bronze/checkpoint/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etapa de instanciação das classes concretas seguindo o Design Pattern Factory.

# COMMAND ----------

# Conversão do schema para o StructType
schema = utils.load_schema("./schema.json")

# Instancia da classe *IngestorFactory*
ingestor = ingestors.IngestorFactory.get_ingestor(file_format, spark, catalog, database, table_name, schema, delimiter, multiline, header)

# Invocação do método *ingest* herdado da interface
ingestor.ingest(ADLS_PATH, EXTERNAL_PATH, CHECKPOINT_PATH)
