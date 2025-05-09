### Tansformaciones DF ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -------------------------
# Función select()
# -------------------------

# Leemos un archivo Parquet en un DataFrame
df = spark.read.parquet("./data/datos.parquet")
df.printSchema()  # Imprime el esquema (columnas y tipos de datos)

from pyspark.sql.functions import col

# Selecciona solo la columna "video_id"
df.select(col("video_id")).show()

# Selecciona dos columnas específicas
df.select("video_id", "trending_date").show()

# Esto está incorrecto porque intenta restar cadenas, no columnas
df.select(
    "likes",
    "dislikes",
    ("likes" - "dislikes")  # ❌ Error si se ejecuta
)

# Forma correcta: resta las columnas likes y dislikes, y le da un alias
df.select(
    col("likes"),
    col("dislikes"),
    (col("likes") - col("dislikes")).alias("aceptación")
).show()

# -------------------------
# Función selectExpr()
# -------------------------

# Usamos expresiones SQL para transformar y renombrar columnas
df.selectExpr("likes", "dislikes", "(likes - dislikes) as aceptacion").show()

# Contamos el número de valores distintos en la columna video_id
df.selectExpr("count(distinct(video_id)) as videos").show()

# -------------------------
# Funciones filter() y where()
# -------------------------

# Muestra todos los registros
df = spark.read.parquet("./data/datos.parquet")
df.show()

# Filtra los registros donde el video_id es igual a un valor específico
df.filter(col("video_id") == "2kyS6SvSYSE").show()

# Filtra donde trending_date es diferente de un valor
df1 = spark.read.parquet("./data/datos.parquet").where(col("trending_date") != "17.14.11")
df1.show()

# Filtra donde los likes son mayores a 5000
df2 = spark.read.parquet("./data/datos.parquet").where(col("likes") > 5000)

# Filtro compuesto con operador lógico AND (&)
df2.filter((col("trending_date") != "17.14.11") & (col("likes") > 7000)).show()

# También se pueden encadenar filtros
df2.filter((col("trending_date") != "17.14.11")).filter((col("likes") > 7000)).show()

# -------------------------
# Función distinct()
# -------------------------

df = spark.read.parquet("./data/datos.parquet")

# Elimina filas duplicadas completamente (todas las columnas)
df_sin_duplicados = df.distinct()

print("El conteo del dataframe original es {}".format(df.count()))
print("El conteo del dataframe sin duplicados es {}".format(df_sin_duplicados.count()))

# -------------------------
# Función dropDuplicates()
# -------------------------

# Creamos un DataFrame de ejemplo
dataframe = spark.createDataFrame([(1, "azul", 567), (2, "rojo", 487), (1, "azul", 345), (2, "verde", 783)]).toDF("ID", "Color", "Importe")
dataframe.show()

# Elimina duplicados solo considerando las columnas "ID" y "Color"
dataframe.dropDuplicates(["ID", "Color"]).show()

# -------------------------
# Función sort()
# -------------------------

from pyspark.sql.functions import col

# Selecciona columnas específicas y elimina duplicados por video_id
df = (spark.read.parquet("./data/datos.parquet")
    .select(col("likes"), col("views"), col("video_id"), col("dislikes"))
    .dropDuplicates(["video_id"])
)

df.show()

from pyspark.sql.functions import desc

# Ordena por la columna likes en orden ascendente
df.sort("likes").show()

# Ordena por la columna likes en orden descendente
df.sort(desc("likes")).show()

# -------------------------
# Función orderBy()
# -------------------------

# Ordena por número de visualizaciones
df.orderBy("views").show()

# Ordena por visualizaciones en orden descendente
df.orderBy(col("views").desc()).show()

# Otro ejemplo con múltiples columnas
dataframe = spark.createDataFrame([(1, "azul", 568), (2, "rojo", 235), (1, "azul", 456), (2, "azul", 783)]).toDF("ID", "Color", "Importe")
dataframe.show()

# Ordena por color descendente y luego por importe ascendente
dataframe.orderBy(col("color").desc(), col("importe")).show()

# -------------------------
# Función limit()
# -------------------------

# Selecciona las 10 filas con más visualizaciones
top_10 = df.orderBy(col("views").desc()).limit(10)
top_10.show()

# -------------------------
# Función withColumn()
# -------------------------

df = spark.read.parquet("./data/datos.parquet")

# Crea una nueva columna llamada "Valoracion" que es la resta de likes y dislikes
df_valoracion = df.withColumn("Valoracion", col("likes") - col("dislikes"))
df_valoracion.printSchema()

# Añade otra columna adicional llamada "res_div" que es el residuo de la división por 10
df_valoracion1 = (df.withColumn("Valoracion", col("likes") - col("dislikes"))
                    .withColumn("res_div", col("Valoracion") % 10)
                )
df_valoracion1.printSchema()
df_valoracion1.select(col("likes"), col("dislikes"), col("Valoracion"), col("res_div")).show()

# -------------------------
# Función withColumnRenamed()
# -------------------------

# Renombra una columna existente
df_renombrado = df.withColumnRenamed("video_id", "ID")
df_renombrado.printSchema()

# Si se intenta renombrar una columna que no existe, no lanza error, simplemente no hace nada
df_error = df.withColumnRenamed("nombre_que_no_existe", "otro_nombre")
df_error.printSchema()

# -------------------------
# Función drop() — elimina columnas
# -------------------------

df = spark.read.parquet("./data/datos.parquet")
df.printSchema()

# Elimina una columna del DataFrame
df_util = df.drop("comments_disabled")
df_util.printSchema()

# Elimina varias columnas a la vez
df_util = df.drop("comments_disabled", "ratings_disabled", "thumbnail_link")
df_util.printSchema()

# Si alguna columna no existe, tampoco lanza error
df_util = df.drop("comments_disabled", "ratings_disabled", "thumbnail_link", "cafe")
df_util.printSchema()

# -------------------------
# Función sample() — toma una muestra aleatoria
# -------------------------

# Devuelve una muestra aleatoria del 80% del DataFrame
df_muestra = df.sample(0.8)

num_filas = df.count()
num_filas_muestra = df_muestra.count()

print("El 80% de filas del dataframe original es {}".format(num_filas - (num_filas*0.2)))
print("El numero de filas del dataframe muestra es {}".format(num_filas_muestra))

# Se puede fijar la semilla para tener una muestra reproducible
df_muestra = df.sample(fraction=0.8, seed=1234)

# También se puede hacer con reemplazo (con repetición)
df_muestra = df.sample(withReplacement=True, fraction=0.8, seed=1234)

# -------------------------
# Función randomSplit() — divide en conjuntos aleatorios
# -------------------------

# Divide el DataFrame original en dos subconjuntos (80% entrenamiento, 20% test)
train, test = df.randomSplit([0.8, 0.2], seed=1234)

