from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp
from abc import ABC, abstractmethod

class ETLBase(Transformer, ABC):
    """
        Interface que define o contrato para as classes concretas.
        Implementa a interface obrigatória para Transformers do PySpark.
    """

    @abstractmethod
    def _transform(self, dataframe: DataFrame) -> DataFrame:
        """
            Método obrigatório que deve ser implementado pelas subclasses.
        """
        pass


class RemoveNullsAndDuplicates(ETLBase):
    """
    Classe que remove registros nulos e duplicados do DataFrame.

    Métodos:
    - Construtor: Recebe como parâmetro as colunas-alvo para remoção.
    - Transform: Remove registros nulos e duplicados.
    """
    def __init__(self, subset_cols: list = None):
        super().__init__()
        if subset_cols is not None and not isinstance(subset_cols, list):
            raise ValueError("O parâmetro 'subset_cols' deve ser uma lista ou None.")
        self.subset_cols = subset_cols

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")
        if self.subset_cols:
            missing_cols = [col for col in self.subset_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"As colunas {missing_cols} não existem no DataFrame.")
        
        # Removendo registros nulos e duplicados
        return df.dropna(how="any", subset=self.subset_cols).dropDuplicates(subset=self.subset_cols)


class FillNulls(ETLBase):
    """
    Classe que preenche valores nulos de uma lista de colunas.

    Métodos:
    - Construtor: Recebe as colunas-alvo e o valor de preenchimento.
    - Transform: Preenche registros nulos.
    """
    def __init__(self, subset_cols: list = None, value_fill=None):
        super().__init__()
        if subset_cols is not None and not isinstance(subset_cols, list):
            raise ValueError("O parâmetro 'subset_cols' deve ser uma lista ou None.")
        if value_fill is None:
            raise ValueError("O parâmetro 'value_fill' não pode ser None.")
        self.subset_cols = subset_cols
        self.value = value_fill

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")
        if self.subset_cols:
            missing_cols = [col for col in self.subset_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"As colunas {missing_cols} não existem no DataFrame.")
        
        # Preenchendo valores nulos
        return df.fillna(self.value, subset=self.subset_cols)



class NormalizeTimestamp(ETLBase):
    """
    Classe que normaliza o timestamp de uma coluna.

    Métodos:
    - Construtor: Recebe a coluna de timestamp e o formato de normalização.
    - Transform: Normaliza o timestamp diretamente na coluna fornecida.
    """
    def __init__(self, input_col: str, estrutura_norm: str):
        super().__init__()
        if not isinstance(input_col, str):
            raise ValueError("O parâmetro 'input_col' deve ser uma string.")
        if not isinstance(estrutura_norm, str):
            raise ValueError("O parâmetro 'estrutura_norm' deve ser uma string.")
        self.input_col = input_col
        self.normalizacao = estrutura_norm

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")
        if self.input_col not in df.columns:
            raise ValueError(f"A coluna '{self.input_col}' não existe no DataFrame.")
        
        # Normalizando o timestamp
        return df.withColumn(self.input_col, to_timestamp(df[self.input_col], self.normalizacao))





