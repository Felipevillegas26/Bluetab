from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, year

class LectorConEsquema:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def leer_con_esquema(self, ruta: str, esquema: StructType) -> DataFrame:
        return self.spark.read.schema(esquema).json(ruta)

class AnalizadorClientes:
    def __init__(self, df_clientes: DataFrame):
        self.df_clientes = df_clientes

    def clientes_mayores_de_edad(self) -> DataFrame:
        return self.df_clientes.filter(col("edad") >= 18)

    def agregar_columna_anio_registro(self) -> DataFrame:
        return self.df_clientes.withColumn("anio_registro", year(col("fecha_registro")))

    def resumen_por_ciudad(self) -> DataFrame:
        return self.df_clientes.groupBy("ciudad").count()

    def clientes_por_ciudad(self, ciudad: str) -> DataFrame:
        return self.df_clientes.filter(col("ciudad") == ciudad)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("EjemploConEsquema").getOrCreate()

    # Definir el esquema manualmente
    esquema_clientes = StructType([
        StructField("id", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("edad", IntegerType(), True),
        StructField("ciudad", StringType(), True),
        StructField("fecha_registro", DateType(), True)
    ])

    lector = LectorConEsquema(spark)
    df_clientes = lector.leer_con_esquema("ruta/clientes.json", esquema_clientes)

    analizador = AnalizadorClientes(df_clientes)

    # Ejemplo de uso de métodos
    mayores = analizador.clientes_mayores_de_edad()
    mayores.show()

    con_anio = analizador.agregar_columna_anio_registro()
    con_anio.show()

    resumen = analizador.resumen_por_ciudad()
    resumen.show()

    clientes_bogota = analizador.clientes_por_ciudad("Bogotá")
    clientes_bogota.show()
