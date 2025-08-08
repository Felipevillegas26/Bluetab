from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

class TablaTransformaciones:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def cargar_datos(self) -> tuple[DataFrame, DataFrame]:
        # Simulación de carga de datos
        tabla_principal = self.spark.read.parquet("ruta/tabla_principal.parquet")
        tabla_auxiliar = self.spark.read.parquet("ruta/tabla_auxiliar.parquet")
        return tabla_principal, tabla_auxiliar

    def transformar(self, df_principal: DataFrame, df_auxiliar: DataFrame) -> DataFrame:
        # JOIN con condición
        df_joined = df_principal.join(
            df_auxiliar,
            on=col("df_principal.id") == col("df_auxiliar.fk_id"),
            how="left"
        )

        # Aplicación de lógica condicional
        df_transformado = df_joined.withColumn(
            "estado_final",
            when(col("df_auxiliar.estado") == "activo", "validado")
            .when(col("df_auxiliar.estado").isNull(), "sin datos")
            .otherwise("rechazado")
        )

        # Filtro por condición
        df_filtrado = df_transformado.filter(col("df_principal.fecha") >= "2023-01-01")

        # Selección final de columnas
        df_final = df_filtrado.select(
            col("df_principal.id").alias("id"),
            col("df_principal.nombre"),
            col("df_auxiliar.estado"),
            col("estado_final")
        )

        return df_final

# Uso de la clase
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Transformaciones PySpark") \
        .getOrCreate()

    transformador = TablaTransformaciones(spark)
    df_principal, df_auxiliar = transformador.cargar_datos()
    df_resultado = transformador.transformar(df_principal, df_auxiliar)
    df_resultado.show()

#holaaaa
#cambio ultimo