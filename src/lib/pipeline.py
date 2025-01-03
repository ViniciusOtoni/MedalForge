from pyspark.ml import Pipeline
import ETL  

class DynamicPipe:

    """
        Classe para criação dinâmica de pipeline a partir de uma lista de classes de transformação.
    """

    def __init__(self, df, steps: list, config: dict):
        self.df = df  # DataFrame original
        self.steps = steps  # Lista de nomes das classes de transformação
        self.config = config  # Arquivo YAML com as configurações do ETL
        self.transform_stages = []  # Lista de instâncias de transformações

    def create_pipeline(self):
        
        """
            Método responsável pela instância das classes de transformação e criação do Pipeline.
        """

        ETL_class_map = {
            'remove_duplicates': ETL.RemoveDuplicates,
            'remove_nulls': ETL.RemoveNulls,
            'convert_types': ETL.ConvertDataType,
            'null_handling': ETL.HandleNullValues,
            'normalize_text': ETL.NormalizeText,
            'normalize_timestamp': ETL.NormalizeTimestamp
        }
        
        # Instâncias das classes de transformação
        for ETL_class_name in self.steps:
            
            if ETL_class_name not in ETL_class_map:
                raise ValueError(f"Classe de transformação '{ETL_class_name}' não encontrada.")

            # Realiza a instância de cada classe passando seus respectivos parâmetros
            instance = ETL_class_map.get(ETL_class_name)(self.config)

            self.transform_stages.append(instance)

        return Pipeline(stages=self.transform_stages)  # Executado de forma sequencial



    def fit_pipeline(self, pipeline, df):
        """
            Ajusta o pipeline ao DataFrame.
        """
        return pipeline.fit(df)

    def transform_pipeline(self, model, df):
        """
            Aplica as transformações do pipeline ao DataFrame.
        """
        return model.transform(df)

    def execute(self):
        """
            Execução do Pipeline.
        """
        # Criar o pipeline
        pipeline = self.create_pipeline()

        # Ajustar o pipeline ao DataFrame original
        model = self.fit_pipeline(pipeline, self.df)  # PipelineModel

        # Aplicar as transformações e retornar o DataFrame transformado
        return self.transform_pipeline(model, self.df)
    


