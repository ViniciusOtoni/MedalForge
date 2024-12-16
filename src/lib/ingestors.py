from abc import ABC, abstractmethod
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
    def ingest(self, source_directory: str, external_location: str, checkpoint_path: str, partitions: list):
        """
            Método responsável por ingerir os dados.
            
            :param source_directory: Diretório que será monitorado pelo AutoLoader.
            :param external_location: Caminho onde os dados serão armazenados no formato Delta.
            :param checkpoint_path: Caminho do checkpoint para AutoLoader.
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

    def ingest(self, source_directory: str, external_location: str, checkpoint_path: str, partitions: list):
        """
            Ingestão de dados de arquivo CSV para o formato Delta.
            
            :param source_directory: Diretório que será monitorado pelo AutoLoader..
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
            :param checkpoint_path: Caminho do checkpoint para AutoLoader.
            :partitions: Lista de colunas para particionamento.
        """

        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            (self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", str(self.header))
                .option("delimiter", self.delimiter)
                .option("cloudFiles.schemaLocation", checkpoint_path)
                .schema(self.schema)
                .load(source_directory)
                .writeStream
                .format("delta")
                .partitionBy(*partitions)
                .option("checkpointLocation", checkpoint_path)
                .start(external_location))

            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, external_location)
        except Exception as e:
            print(f"Erro ao processar o arquivo CSV: {e}")


class JSONIngestor(SemiStructuredDataIngestor):
    """
        Classe concreta para ingestão de arquivos JSON. Herda de SemiStructuredDataIngestor.
    """

    def ingest(self, source_directory: str, external_location: str, checkpoint_path: str, partitions: list):
         """
            Ingestão de dados de arquivo JSON para o formato Delta.
            
            :param source_directory: Diretório que será monitorado pelo AutoLoader..
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
            :param checkpoint_path: Caminho do checkpoint para AutoLoader.
            :partitions: Lista de colunas para particionamento.
        """

        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            (self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("multiline", str(self.multiline))
                .option("cloudFiles.schemaLocation", checkpoint_path)
                .option("mode", "PERMISSIVE") 
                .schema(self.schema)
                .load(source_directory)
                .writeStream
                .format("delta")
                .partitionBy(*partitions)
                .option("checkpointLocation", checkpoint_path)
                .start(external_location))

            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, external_location)
        except Exception as e:
            print(f"Erro ao processar o arquivo JSON: {e}")


class TextIngestor(UnstructuredDataIngestor):
    """
       Classe concreta para ingestão de arquivos de texto. Herda de UnstructuredDataIngestor.
    """

    def ingest(self, source_directory: str, external_location: str, checkpoint_path: str, partitions: list):
         """
            Ingestão de dados de arquivo TEXT para o formato Delta.
            
            :param source_directory: Diretório que será monitorado pelo AutoLoader..
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
            :param checkpoint_path: Caminho do checkpoint para AutoLoader.
            :partitions: Lista de colunas para particionamento.
        """

        if partitions is None:
            partitions = []  # Define um valor padrão vazio para partitions

        try:
            # Lendo os arquivos de texto usando AutoLoader (streaming)
            text_df = (self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "text")
                .option("cloudFiles.schemaLocation", checkpoint_path)
                .load(source_directory))
            
            # Dividindo o valor da coluna "value" pelo delimitador especificado
            split_df = text_df.selectExpr(
                [f"split(value, '{self.delimiter}')[{i}] as col{i}" for i in range(len(self.schema.fields))]
            )

            # Aplicando o schema dinamicamente
            for i, field in enumerate(self.schema.fields):
                split_df = split_df.withColumn(field.name, split_df[f"col{i}"].cast(field.dataType))

            # Selecionando as colunas finais de acordo com o schema
            final_df = split_df.select([field.name for field in self.schema.fields])

            # Salvando os dados no formato Delta com particionamento (streaming)
            query = (final_df.writeStream
                .format("delta")
                .partitionBy(*partitions)
                .option("checkpointLocation", checkpoint_path)
                .start(external_location))

            utils.create_external_table(self.spark, self.catalog, self.database, self.table_name, external_location)

            query.awaitTermination()  # Espera a execução da query de streaming
            
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
