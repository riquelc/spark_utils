from pyspark.sql import SparkSession
from spark_utils.flatten import aplanar_json

# Inicializando Spark
spark = SparkSession.builder.appName("Example").getOrCreate()

def exemplo_uso():
    # Criando um exemplo de DataFrame com estrutura aninhada
    data = [
        ("1", {
            "travelerPricings": {
                "fareDetailsBySegment": {
                    "amenities": [{"isChargeable": True}],
                    "fees": [{"isChargeable": False}]
                }
            }
        })
    ]
    
    schema = "id STRING, data STRUCT<travelerPricings: STRUCT<fareDetailsBySegment: STRUCT<amenities: ARRAY<STRUCT<isChargeable: BOOLEAN>>, fees: ARRAY<STRUCT<isChargeable: BOOLEAN>>>>>"
    
    df = spark.createDataFrame(data, schema)
    
    print("DataFrame original:")
    df.printSchema()
    
    # Aplanando o DataFrame
    df_aplanado = aplanar_json(df)
    
    print("\nDataFrame aplanado:")
    df_aplanado.printSchema()
    
    # Mostrando os dados
    df_aplanado.show(truncate=False)

if __name__ == "__main__":
    exemplo_uso()
