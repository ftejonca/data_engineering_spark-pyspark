### Escritura de dataframes ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Carga un DataFrame desde un archivo Parquet ubicado en la ruta local "./data/datos.parquet"
df = spark.read.parquet("./data/datos.parquet")

# Reparte el DataFrame en 2 particiones. Esto puede mejorar el rendimiento al escribir o procesar datos en paralelo
df1 = df.repartition(2)

# Escribe el DataFrame df1 en formato CSV, usando el carácter '|' como separador
# Al haber 2 particiones, se generarán 2 archivos de salida CSV en la carpeta "./output/csv"
df1.write.format("csv").option("sep", "|").save("./output/csv")

# Une (coalesce) las particiones a 1 sola antes de escribir. Así se genera solo 1 archivo CSV
df1.coalesce(1).write.format("csv").option("sep", "|").save("./output/csv1")

# Imprime el esquema (estructura) del DataFrame, mostrando nombre de columnas y tipos de datos
df.printSchema()

# Muestra los valores distintos de la columna "comments_disabled"
df.select("comments_disabled").distinct().show()

# Importa la función col para construir expresiones de filtrado
from pyspark.sql.functions import col

# Filtra el DataFrame para conservar solo las filas donde "comments_disabled" sea "True" o "False"
df_limpio = df.filter(col("comments_disabled").isin("True", "False"))

# Verifica que el filtrado fue exitoso mostrando los valores únicos de "comments_disabled"
df_limpio.select("comments_disabled").distinct().show()

# Escribe el DataFrame limpio en formato Parquet, particionando por la columna "comments_disabled"
# Esto genera subcarpetas como comments_disabled=True/ y comments_disabled=False/
df_limpio.write.partitionBy("comments_disabled").parquet("./output/parquet")
