from abc import ABC, abstractmethod
from pyspark.sql import functions as F
from datetime import datetime
import utils


class DataIngestor(ABC):
    """
        Interface principal para ingestão de dados.

        Define o contrato para a ingestão de diferentes tipos de dados (estruturado, semiestruturado, não estruturado).
        Todas as classes concretas de ingestão devem implementar o método `ingest`.
    """

    def __init__(self, spark, catalog, database, table_name):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table_name = table_name

    @abstractmethod
    def ingest(self, source_directory: str, external_location: str, partitions: list):
        """
            Método responsável por ingerir os dados.
            
            :param source_directory: Diretório que será monitorado pelo AutoLoader.
            :param external_location: Caminho onde os dados serão armazenados no formato Delta.
            :param partitions: Lista de colunas para particionamento.
        """
        pass


class StructuredDataIngestor(DataIngestor, ABC):
    """
        Interface para ingestão de dados estruturados (ex: CSV).
    """ 

    def __init__(self, spark, catalog, database, table_name, schema, delimiter, header):
        super().__init__(spark, catalog, database, table_name)  # Chama o construtor da classe base
        self.schema = schema
        self.delimiter = delimiter
        self.header = header


class SemiStructuredDataIngestor(DataIngestor, ABC):
    """
        Interface para ingestão de dados semiestruturados (ex: JSON).
    """

    def __init__(self, spark, catalog, database, table_name, schema, multiline=False):
        super().__init__(spark, catalog, database, table_name)  # Chama o construtor da classe base
        self.schema = schema
        self.multiline = multiline


class UnstructuredDataIngestor(DataIngestor, ABC):
    """
        Interface para ingestão de dados não estruturados (ex: arquivos de texto).
    """

    def __init__(self, spark, catalog, database, table_name, schema, delimiter):
        super().__init__(spark, catalog, database, table_name)  # Chama o construtor da classe base
        self.schema = schema
        self.delimiter = delimiter


# ------------------------------------------------------------------------------------------------------------- #

class CSVIngestor(StructuredDataIngestor):
    """
        Classe concreta para ingestão de arquivos CSV. Herda de StructuredDataIngestor.
    """

    def ingest(self, source_directory: str, base_bronze_path: str, partitions: list):
        """
        Ingestão de dados de arquivo CSV para o formato Delta em modo batch.
        :param source_directory: Diretório de origem dos arquivos CSV.
        :param base_bronze_path: Caminho base da camada Bronze.
        :param partitions: Lista de colunas para particionamento.
        """
        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            # Gerar o caminho dinâmico com a data de ingestão
            current_date = datetime.now().strftime("%Y-%m-%d")
            bronze_path = f"{base_bronze_path}/{self.table_name}/{current_date}/"

            # Leitura dos arquivos CSV e gravação direta no formato Delta
            (self.spark.readStream
                .format("cloudFiles") # Monitoramento do AutoLoader
                .option("cloudFiles.format", "csv")
                .option("header", str(self.header))  # Define que o arquivo tem cabeçalho
                .option("delimiter", self.delimiter)  # Define o delimitador
                .option("nullValue", "")  
                .schema(self.schema)
                .load(source_directory)  # Leitura
                .writeStream
                .format("delta")  # Salva diretamente em Delta
                .partitionBy(*partitions)
                .outputMode("append")
                .option("checkpointLocation", f"{base_bronze_path}/{self.table_name}/_checkpoint") # checkpoint path
                .trigger(once=True) 
                .start(bronze_path)
                .awaitTermination()) # Espera o termino da ingestão para "desligar"

            # Criação da tabela externa no Unity Catalog
            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, bronze_path)
            print(f"Ingestão concluída no diretório: {bronze_path}")
        except Exception as e:
            print(f"Erro ao processar o arquivo CSV: {e}")



class JSONIngestor(SemiStructuredDataIngestor):
    """
        Classe concreta para ingestão de arquivos JSON. Herda de SemiStructuredDataIngestor.
    """

    def ingest(self, source_directory: str, base_bronze_path: str, partitions: list):
        """
            Ingestão de dados de arquivo JSON para o formato Delta em modo batch.

            :param source_directory: Diretório de origem dos arquivos JSON.
            :param base_bronze_path: Caminho base da camada Bronze.
            :param partitions: Lista de colunas para particionamento.
        """

        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            # Gerar o caminho dinâmico com a data de ingestão
            current_date = datetime.now().strftime("%Y-%m-%d")
            bronze_path = f"{base_bronze_path}/{self.table_name}/{current_date}/"

            # Leitura dos arquivos JSON e gravação direta no formato Delta
            (self.spark.readStream
                .format("cloudFiles") # Monitoramento do AutoLoader
                .option("cloudFiles.format", "json")
                .option("multiline", str(self.multiline)) # Opção de dado multilinha (True | False)
                .schema(self.schema) # Define schema
                .load(source_directory) # Leitura
                .writeStream
                .format("delta") # Salva diretamente em Delta 
                .option("checkpointLocation", f"{base_bronze_path}/{self.table_name}/_checkpoint") # checkpoint path
                .partitionBy(*partitions)
                .outputMode("append")
                .trigger(once=True) 
                .start(bronze_path)
                .awaitTermination()) # Espera o termino da ingestão para "desligar"

            # Criação da tabela externa no Unity Catalog
            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, bronze_path)
            print(f"Ingestão concluída no diretório: {bronze_path}")
        except Exception as e:
            print(f"Erro ao processar o arquivo JSON: {e}")


class TextIngestor(UnstructuredDataIngestor):
    """
       Classe concreta para ingestão de arquivos de texto delimitados. Herda de UnstructuredDataIngestor.
    """

    def ingest(self, source_directory: str, base_bronze_path: str, partitions: list = None):
        """
            Ingestão de dados de arquivo TXT para o formato Delta em modo batch.

            :param source_directory: Diretório de origem dos arquivos TXT.
            :param base_bronze_path: Caminho base da camada Bronze.
            :param partitions: Lista de colunas para particionamento.
        """

        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            # Gerar o caminho dinâmico com a data de ingestão
            current_date = datetime.now().strftime("%Y-%m-%d")
            bronze_path = f"{base_bronze_path}/{self.table_name}/{current_date}/"

            # Leitura dos arquivos TXT e gravação direta no formato Delta
            (self.spark.readStream
                .format("cloudFiles")  
                .option("cloudFiles.format", "text")
                .load(source_directory)  # Leitura
                .withColumn("splitted_values", F.split(F.col("value"), self.delimiter))  # Divide a coluna "value"
                # Aplica o esquema carregado dinamicamente
                .select(*[F.col("splitted_values")[i].alias(self.schema.fields[i].name)
                          for i in range(len(self.schema.fields))])  # Aplica o esquema a cada coluna
                .writeStream
                .format("delta") # Salva diretamente em Delta
                .option("checkpointLocation", f"{base_bronze_path}/{self.table_name}/_checkpoint") # checkpoint path
                .partitionBy(*partitions)
                .outputMode("append")
                .trigger(once=True) 
                .start(bronze_path)
                .awaitTermination())  # Espera o termino da ingestão para "desligar"

            # Criação da tabela externa no Unity Catalog
            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, bronze_path)
            print(f"Ingestão concluída no diretório: {bronze_path}")
        except Exception as e:
            print(f"Erro ao processar o arquivo de texto: {e}")




# ------------------------------------------------------------------------------------------------------------- #

class IngestorFactory:
    """
        Método para obter o Ingestor apropriado com base no tipo de arquivo.
        
        :param file_type: Tipo de arquivo (por exemplo, 'csv', 'json', 'text').
        :param spark: Sessão do Spark.
        :param catalog: Nome do catálogo de dados.
        :param database: Nome do database.
        :param table_name: Nome da tabela.
        :param schema: Schema a ser aplicado durante a ingestão.
        :param delimiter: Delimitador para arquivos como CSV ou texto.
        :param multiline: Flag que indica se o arquivo JSON é multiline.
        :param header: Flag para indicar se o arquivo CSV possui cabeçalho.
        
        :return: Instância do Ingestor apropriado.
    """

    @staticmethod
    def get_ingestor(file_type: str, spark, catalog, database, table_name, schema, delimiter=None, multiline=False, header=True):
        if file_type == 'csv':
            return CSVIngestor(spark, catalog, database, table_name, schema, delimiter, header)
        elif file_type == 'json':
            return JSONIngestor(spark, catalog, database, table_name, schema, multiline)
        elif file_type == 'text':
            return TextIngestor(spark, catalog, database, table_name, schema, delimiter)
        else:
            raise ValueError(f"Tipo de arquivo desconhecido: {file_type}")


