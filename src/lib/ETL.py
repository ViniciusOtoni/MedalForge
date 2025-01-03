from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lower, trim, avg, expr
from abc import ABC, abstractmethod



class ETLBase(Transformer, ABC):
    """
    Interface que define o contrato para as classes concretas.
    Implementa a interface obrigatória para Transformers do PySpark.
    """

    def __init__(self, config: dict):
        """
        :param config: Arquivo YAML com as regras de configuração.
        """
        super().__init__()
        self.config = config

    @abstractmethod
    def _transform(self, dataframe: DataFrame) -> DataFrame:
        """
        Método obrigatório que deve ser implementado pelas subclasses.
        """
        pass


class RemoveDuplicates(ETLBase):
    """
    Classe que remove registros duplicados do DataFrame.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.subset_cols = self.config.get("remove_duplicates", {}).get("subset_cols", None)

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")

        # Remover duplicatas considerando as colunas fornecidas ou todas as colunas (se subset for None)
        return df.dropDuplicates(self.subset_cols)
        


class RemoveNulls(ETLBase):
    """
    Classe que remove registros nulos do DataFrame.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.subset_cols = self.config.get("remove_nulls", {}).get("subset_cols", None)
        self.how = self.config.get("remove_nulls", {}).get("how", "any")

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")

        # Remover registros nulos considerando as colunas fornecidas ou todas as colunas (se subset for None)
        return df.dropna(how=self.how, subset=self.subset_cols)


class ConvertDataType(ETLBase):
    """
    Classe para conversão de tipos de dados em colunas específicas.
    """

    def __init__(self, config: dict):
        super().__init__(config)

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")

        for col_name, new_type in self.config.get("convert_types", {}).items():
            if col_name not in df.columns:
                raise ValueError(f"A coluna '{col_name}' não existe no DataFrame.")
            df = df.withColumn(col_name, col(col_name).cast(new_type))

        return df
    
class HandleNullValues(ETLBase):
    """
    Classe para lidar com valores nulos, permitindo preenchimento com média, mediana ou valor predefinido.
    """

    def __init__(self, config: dict):
        super().__init__(config)

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")

        for col_name, strategy in self.config.get("null_handling", {}).items():
            if col_name not in df.columns:
                raise ValueError(f"A coluna '{col_name}' não existe no DataFrame.")

            if strategy == "mean":
                mean_value = df.select(avg(col_name)).first()[0]
                df = df.fillna({col_name: mean_value})
            elif strategy == "median":
                median_value = df.approxQuantile(col_name, [0.5], 0.01)[0]
                df = df.fillna({col_name: median_value})
            elif isinstance(strategy, (int, float, str)):
                df = df.fillna({col_name: strategy})
            else:
                raise ValueError(f"Estratégia '{strategy}' inválida para a coluna '{col_name}'.")

        return df

class NormalizeText(ETLBase):
    """
    Classe para normalização de texto, como remoção de espaços extras e conversão para letras minúsculas.
    """

    def __init__(self, config: dict):
        super().__init__(config)

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")

        for col_name in self.config.get("normalize_text", []):
            if col_name not in df.columns:
                raise ValueError(f"A coluna '{col_name}' não existe no DataFrame.")

            # Aplicando normalização
            df = df.withColumn(col_name, lower(trim(col(col_name))))

        return df

class NormalizeTimestamp(ETLBase):
    """
    Classe que normaliza o timestamp de uma coluna.
    """

    def __init__(self, config: dict):
        super().__init__(configf)
        self.input_col = self.config.get("input_col", None) if self.config else None
        self.format = self.config.get("format", "yyyy-MM-dd HH:mm:ss") if self.config else None

    def _transform(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame válido.")
        if self.input_col not in df.columns:
            raise ValueError(f"A coluna '{self.input_col}' não existe no DataFrame.")

        # Normalizando o timestamp
        return df.withColumn(self.input_col, to_timestamp(df[self.input_col], self.format))
    


