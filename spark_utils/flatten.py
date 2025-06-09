from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType, DataType
from pyspark.sql import DataFrame
import re

def obter_campos_complexos(df: DataFrame) -> dict:
    """Recupera todos os campos complexos (StructType ou ArrayType) do esquema do DataFrame."""
    return {field.name: field.dataType for field in df.schema.fields 
            if isinstance(field.dataType, (ArrayType, StructType))}

def extrair_nome_niveis(caminho_completo: str) -> str:
    """Extrai os dois últimos níveis da estrutura.
    
    Args:
        caminho_completo: Caminho completo do campo (ex: 'travelerPricings_fareDetailsBySegment_amenities_isChargeable')
        
    Returns:
        Nome composto dos dois últimos níveis (ex: 'amenities_isChargeable')
    """
    partes = caminho_completo.split('_')
    if len(partes) <= 2:
        return partes[-1]
    return f"{partes[-2]}_{partes[-1]}"

def extrair_nome_completo(caminho_completo: str) -> str:
    """Extrai o nome completo mantendo toda a hierarquia.
    
    Args:
        caminho_completo: Caminho completo do campo (ex: 'travelerPricings_fareDetailsBySegment_amenities_isChargeable')
        
    Returns:
        Nome completo mantendo toda a hierarquia (ex: 'travelerPricings_fareDetailsBySegment_amenities_isChargeable')
    """
    return caminho_completo

def aplanar_json(df: DataFrame, modo='reduzido') -> DataFrame:
    """Aplana estruturas JSON aninhadas em um DataFrame Spark.
    
    Args:
        df: DataFrame de entrada contendo estruturas JSON aninhadas
        modo: Modo de aplanamento:
            - 'reduzido': Mantém apenas os dois últimos níveis da hierarquia
            - 'completo': Mantém toda a hierarquia completa
            
    Returns:
        DataFrame com todas as estruturas aninhadas aplanadas
        
    Raises:
        ValueError: Se a entrada não for um DataFrame válido ou modo inválido
        Exception: Para erros durante o processamento
    """
    if not isinstance(df, DataFrame):
        raise ValueError("A entrada deve ser um DataFrame Spark")
    if modo not in ['reduzido', 'completo']:
        raise ValueError("Modo deve ser 'reduzido' ou 'completo'")
    
    campos_complexos = obter_campos_complexos(df)
    
    while campos_complexos:
        try:
            nome_coluna, tipo_coluna = next(iter(campos_complexos.items()))
            print(f"Processando: {nome_coluna}, Tipo: {type(tipo_coluna)}")
            
            if isinstance(tipo_coluna, StructType):
                if modo == 'reduzido':
                    colunas_expandidas = [
                        col(f"{nome_coluna}.{sub_field.name}").alias(extrair_nome_niveis(sub_field.name))
                        for sub_field in tipo_coluna
                    ]
                else:  # modo == 'completo'
                    colunas_expandidas = [
                        col(f"{nome_coluna}.{sub_field.name}").alias(extrair_nome_completo(sub_field.name))
                        for sub_field in tipo_coluna
                    ]
                df = df.select("*", *colunas_expandidas).drop(nome_coluna)
                
            elif isinstance(tipo_coluna, ArrayType):
                df = df.withColumn(nome_coluna, explode_outer(nome_coluna))
                
            campos_complexos = obter_campos_complexos(df)
            
        except Exception as e:
            print(f"Erro processando coluna {nome_coluna}: {str(e)}")
            raise
    
    return df
