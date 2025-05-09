### Trabajo con datos incorrectos o faltantes ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Lee un DataFrame en formato Parquet desde el directorio local ./data
df = spark.read.parquet("./data/datos.parquet")

# Cuenta el número total de registros en el DataFrame (incluyendo valores nulos)
df.count()

# Elimina cualquier fila que tenga al menos un valor nulo y cuenta las filas restantes
df.na.drop().count()

# Hace lo mismo que arriba: elimina filas con al menos un valor nulo ('any' es el valor por defecto)
df.na.drop("any").count()

# dropna() es un alias de na.drop(), así que el comportamiento es el mismo
df.dropna().count()

# Elimina filas que tengan valores nulos solo en la columna "views"
df.na.drop(subset=["views"]).count()

# Elimina filas que tengan valores nulos en al menos una de las columnas "views" o "dislikes"
df.na.drop(subset=["views", "dislikes"]).count()

# Importa la función col para trabajar con columnas
from pyspark.sql.functions import col

# Ordena por la columna "views" y selecciona solo las columnas views, likes y dislikes
df.orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()

# Rellena todos los valores nulos del DataFrame con 0, luego ordena por "views" y muestra las columnas seleccionadas
df.fillna(0).orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()

# Rellena solo los valores nulos de las columnas "likes" y "dislikes" con 0, luego ordena y muestra
df.fillna(0, subset=["likes", "dislikes"]).orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()