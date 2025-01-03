import json
from pyspark.sql.types import StructType, StructField, StringType
import yaml

def create_external_table(spark, catalog, database, table_name, external_location: str):
    
    """
        Cria uma tabela externa no Databricks apontando para o local Delta.
        
        :param spark: Sessão Spark.
        :param catalog: Nome do catálogo de dados.
        :param database: Nome do banco de dados.
        :param table_name: Nome da tabela.
        :param external_location: Caminho onde os dados são armazenados.
    """

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {catalog}.{database};
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table_name}
        USING DELTA
        LOCATION '{external_location}';
    """)
    
    print(f"External Delta Table '{catalog}.{database}.{table_name}' criada com sucesso!")


# Função para carregar o schema a partir de um arquivo JSON e converter para StructType
def load_schema(schema_path: str):
    with open(schema_path, 'r') as file:
        schema_json = json.load(file)
    
    return json_to_struct_type(schema_json)

# Função para converter o JSON em StructType
def json_to_struct_type(schema_json: dict):
    fields = []
    for field in schema_json["columns"]:
        # Forçar todos os tipos para StringType (camada Bronze)
        data_type = StringType()
        
        # Adiciona a coluna com StringType para todas as colunas
        fields.append(StructField(field["name"], data_type, True))
    
    return StructType(fields)


# Função para carregar arquivo YAML e converter para um dicionário
def load_config(config_path: str) -> dict:
        """
        Carrega a configuração de um arquivo YAML.
        :param config_path: Caminho para o arquivo YAML.
        :return: Dicionário com a configuração.
        """
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)



