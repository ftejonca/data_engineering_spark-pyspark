### Trabajo con columnas ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Se lee un archivo en formato Parquet (formato columnar eficiente) y se carga como DataFrame
df = spark.read.parquet("./data/dataPARQUET2.parquet")

# Se imprime el esquema del DataFrame, mostrando los nombres de las columnas y sus tipos de datos
df.printSchema()

# -------- Selección de columnas --------

# Primera forma de seleccionar una columna: usando su nombre como string directamente
df.select("title").show()  # Muestra el contenido de la columna "title"

# Segunda forma de seleccionar una columna: usando la función col() de pyspark.sql.functions
from pyspark.sql.functions import col

# Se selecciona la columna "title" utilizando col(), útil cuando se necesitan operaciones más complejas o dinámicas
df.select(col("title")).show()