### Acciones sobre un dataframe en Spark SQL ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Carga un DataFrame desde un archivo Parquet ubicado en la ruta local "./data/datos.parquet"
df = spark.read.parquet("./data/datos.parquet")

# Muestra las primeras 20 filas del DataFrame por defecto (truncate=True limita el ancho de las columnas largas)
df.show()

# Muestra solo las primeras 5 filas (truncate sigue siendo True por defecto)
df.show(5)

# Muestra las primeras 5 filas sin truncar el contenido de las columnas (útil para texto largo o JSON)
df.show(5, truncate=False)

# ----------------------------
# Otras formas de obtener datos del DataFrame
# ----------------------------

# Función take(n): devuelve una lista con las primeras n filas como objetos Row (útil para inspección rápida)
df.take(1)

# Función head(n): hace lo mismo que take(n); también puede usarse como df.head() para solo la primera fila
df.head(1)

# Función collect(): devuelve todas las filas del DataFrame como una lista de objetos Row
# ⚠️ ¡Cuidado al usar en DataFrames grandes, ya que puede saturar la memoria!
from pyspark.sql.functions import col
df.select(col("likes")).collect()