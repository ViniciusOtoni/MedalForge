import json
from pyspark.sql.types import StructType, StructField, StringType


def create_external_table(self, external_location: str):
    
    """
        Cria uma tabela externa no Databricks apontando para o local Delta.
        
        :param external_location: Caminho onde os dados são armazenados.
    """
    self.spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.database};
    """)
    
    self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.{self.table_name}
        USING DELTA
        LOCATION '{external_location}';
    """)
    
    print(f"External Delta Table '{self.catalog}.{self.database}.{self.table_name}' criada com sucesso!")
    

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