from abc import ABC, abstractmethod
import utils

class DataIngestor(ABC):
    
    """
        Interface principal para ingestão de dados.

        Define o contrato para a ingestão de diferentes tipos de dados (estruturado, semiestruturado, não estruturado).
        Todas as classes concretas de ingestão devem implementar o método `ingest`.
    """
    
    @abstractmethod
    def ingest(self, file_path: str, external_location: str):
        """
            Método responsável por ingerir os dados.
            
            :param file_path: Caminho do arquivo de origem.
            :param external_location: Caminho onde os dados serão armazenados no formato Delta.
        """
        pass


class StructuredDataIngestor(DataIngestor):
    
    """
        Interface para ingestão de dados estruturados (ex: CSV).
    """ 
    
    def __init__(self, spark, schema, delimiter, header):
        self.spark = spark
        self.schema = schema
        self.delimiter = delimiter
        self.header = header


class SemiStructuredDataIngestor(DataIngestor):
    
    """
        Interface para ingestão de dados semiestruturados (ex: JSON).
    """
    
    def __init__(self, spark, schema, multiline=False):
        self.spark = spark
        self.schema = schema
        self.multiline = multiline


class UnstructuredDataIngestor(DataIngestor):
    
    """
        Interface para ingestão de dados não estruturados (ex: arquivos de texto).
    """

    def __init__(self, spark, schema, delimiter):
        self.spark = spark
        self.schema = schema
        self.delimiter = delimiter

# ------------------------------------------------------------------------------------------------------------- #

class CSVIngestor(StructuredDataIngestor):
    
    """
        Classe concreta para ingestão de arquivos CSV. Herda de StructuredDataIngestor.
    """

    def ingest(self, file_path: str, external_location: str):
        """
            Ingestão de dados de arquivo CSV para o formato Delta.
            
            :param file_path: Caminho do arquivo CSV.
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
        """
        try:
            (self.spark.read
             .format("csv")
             .option("header", str(self.header))
             .option("delimiter", self.delimiter)
             .schema(self.schema)
             .load(file_path)
             .write
             .format("delta")
             .mode("overwrite")
             .save(external_location))

            # Chama o método auxiliar para criar a tabela externa
            utils.create_external_table(external_location)
            
        except Exception as e:
            print(f"Erro ao processar o arquivo CSV: {e}")


class JSONIngestor(SemiStructuredDataIngestor):
    
    """
        Classe concreta para ingestão de arquivos JSON. Herda de SemiStructuredDataIngestor.
    """

    def ingest(self, file_path: str, external_location: str):
        
        """
            Ingestão de dados de arquivo JSON para o formato Delta.
            
            :param file_path: Caminho do arquivo JSON.
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
        """
        
        # Definindo um schema explícito para o arquivo JSON
        json_schema = self.schema 
        
        try:
            # Tentativa de leitura do arquivo JSON com o schema
            df = (self.spark.read
                  .format("json")
                  .option("multiline", str(self.multiline))  # Dependendo de como o arquivo JSON está estruturado
                  .schema(json_schema)  # Aplica o schema definido
                  .option("mode", "PERMISSIVE")  # Permite erros de formatação e tenta ler o máximo possível
                  .load(file_path))

            # Gerar dados caso a leitura seja bem sucedida
            df.write.format("delta").mode("overwrite").save(external_location)

            # Chama o método auxiliar para criar a tabela externa
            utils.create_external_table(external_location)
        
        except Exception as e:
            print(f"Erro ao processar o arquivo JSON: {e}")

class TextIngestor(UnstructuredDataIngestor):
    
    """
        Classe concreta para ingestão de arquivos de texto. Herda de UnstructuredDataIngestor.
    """

    def ingest(self, file_path: str, external_location: str):
        
        """
            Realiza a ingestão dos dados de texto para o formato Delta, aplicando o schema e delimitador.
            
            :param file_path: Caminho do arquivo de texto.
            :param external_location: Caminho onde os dados serão salvos no formato Delta.
        """
        try:
            # Lendo o arquivo de texto
            text_df = self.spark.read.text(file_path)

            # Dividindo o valor da coluna "value" pelo delimitador especificado
            split_df = text_df.selectExpr(
                [f"split(value, '{self.delimiter}')[{i}] as col{i}" for i in range(len(self.schema.fields))]
            )

            # Aplicando o schema dinamicamente
            for i, field in enumerate(self.schema.fields):
                split_df = split_df.withColumn(field.name, split_df[f"col{i}"].cast(field.dataType))

            # Selecionando as colunas finais de acordo com o schema
            final_df = split_df.select([field.name for field in self.schema.fields])

            # Salvando os dados no formato Delta
            final_df.write.format("delta").mode("overwrite").save(external_location)

            # Criando a tabela externa (assumindo que utils.create_external_table já foi definido)
            utils.create_external_table(external_location)

        except Exception as e:
            print(f"Erro ao processar o arquivo de texto: {e}")

# ------------------------------------------------------------------------------------------------------------- #

class IngestorFactory:

    """
        Factory que cria a instância apropriada de um Ingestor baseado no tipo de arquivo ou dados.
    """

    @staticmethod
    def get_ingestor(file_type: str, spark, schema, delimiter=None, multiline=False, header=True):
        
        """
            Método para obter o Ingestor apropriado com base no tipo de arquivo.
            
            :param file_type: Tipo de arquivo (por exemplo, 'csv', 'json', 'text').
            :param spark: Sessão do Spark.
            :param schema: Schema a ser aplicado durante a ingestão.
            :param delimiter: Delimitador para arquivos como CSV ou texto.
            :param multiline: Flag que indica se o arquivo JSON é multiline.
            :param header: Flag para indicar se o arquivo CSV possui cabeçalho.
            
            :return: Instância do Ingestor apropriado.
        """

        if file_type == 'csv':
            return CSVIngestor(spark, schema, delimiter, header)
        elif file_type == 'json':
            return JSONIngestor(spark, schema, multiline)
        elif file_type == 'text':
            return TextIngestor(spark, schema, delimiter)
        else:
            raise ValueError(f"Tipo de arquivo desconhecido: {file_type}")
