from pyspark.ml import Pipeline
import ETL  

class DynamicPipe:

    """
        Classe para criação dinâmica de pipeline a partir de uma lista de classes de transformação.
    """

    def __init__(self, df, steps: list, parms: dict = None):
        self.df = df  # DataFrame original
        self.steps = steps  # Lista de nomes das classes de transformação
        self.parms = parms or {}  # Parâmetros para as classes
        self.transform_stages = []  # Lista de instâncias de transformações

    def create_pipeline(self):
        
        """
            Método responsável pela instância das classes de transformação e criação do Pipeline.
        """
        # Instâncias das classes de transformação
        for ETL_class_name in self.steps:
            # Obtém a classe do módulo ETL dinamicamente
            ETL_class = getattr(ETL, ETL_class_name)
            
            # Obtém os parâmetros específicos para esta classe, se existirem
            class_parms = self.parms.get(ETL_class_name, {})

            # Realiza a instância de cada classe passando seus respectivos parâmetros
            instance = ETL_class(**class_parms)
            self.transform_stages.append(instance)

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
    


