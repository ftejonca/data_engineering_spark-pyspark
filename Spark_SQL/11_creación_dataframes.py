### Creación de DataFrames ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

## Crear un dataframe a partir de un RDD

# Se crea un RDD con pares (número, su cuadrado)
rdd = sc.parallelize([item for item in range(10)]).map(lambda x: (x, x ** 2))
rdd.collect()  # Devuelve todos los pares para visualizarlos

# Se convierte el RDD a un DataFrame, especificando nombres de columnas
df = rdd.toDF(["número", "cuadrado"])
df.printSchema()  # Muestra la estructura del DataFrame (nombre de columnas y tipo de datos)
df.show()  # Muestra los datos (por defecto, los primeros 20 registros)

# ---------------------------------------

## Crear un dataframe a partir de un RDD con schema personalizado

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Se crea un RDD con tuplas que representan personas con: ID, nombre y edad
rdd1 = sc.parallelize([(1, "Fernando", 36.7), (2, "Yaiza", 32.3), (3, "Ari", 4.9), (4, "Eizan", 4.9), (5, "Ilian", 1.6)])

# Modo 1 para definir un esquema: usando objetos StructType y StructField
esquema1 = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", DoubleType(), True)
])

# Modo 2 para definir un esquema: como string con tipos SQL
esquema2 = "`ID` INT, `Name` STRING, `Age` DOUBLE"

# Se crea un DataFrame a partir del RDD y el esquema definido (modo 1)
df1 = spark.createDataFrame(rdd1, schema=esquema1)
df1.printSchema()  # Muestra la estructura del DataFrame
df1.show()  # Muestra los datos

# Se repite lo anterior, pero usando el esquema como cadena (modo 2)
df2 = spark.createDataFrame(rdd1, schema=esquema2)
df2.printSchema()
df2.show()

# ---------------------------------------

## Crear un dataframe a partir de fuentes de datos

# Leer un archivo de texto como DataFrame (cada línea del archivo es una fila en la columna 'value')
df = spark.read.text("./data/dataTXT.txt")
df.show()  # Muestra el contenido truncado (columna 'value')
df.show(truncate=False)  # Muestra el contenido completo sin cortar texto

# Leer archivo delimitado por '|' e interpretar la primera fila como encabezado
df2 = spark.read.option("header", "true").option("delimiter", "|").csv("./data/dataTab.txt")
df2.show()

# Leer archivo CSV sin opciones (sin encabezados, columnas con nombres por defecto _c0, _c1, etc.)
df1 = spark.read.csv("./data/dataCSV.csv")
df1.show()

# Leer archivo CSV con opción de encabezados (toma la primera fila como nombres de columna)
df1 = spark.read.option("header", "true").csv("./data/dataCSV.csv")
df1.show()

# ---------------------------------------

# Leer un archivo JSON aplicando un esquema previamente definido

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Se define el esquema para los campos que se esperan en el JSON
json_schema = StructType([
    StructField("color", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("fecha", DateType(), True),
    StructField("país", StringType(), True)
])

# Se lee el archivo JSON aplicando el esquema
df4 = spark.read.schema(json_schema).json("./data/dataJSON.json")
df4.show()  # Muestra los datos leídos
df4.printSchema()  # Muestra el esquema aplicado

# ---------------------------------------

# Leer un archivo Parquet (formato binario columnar optimizado)

df5 = spark.read.parquet("./data/dataPARQUET.parquet")
df5.show()

# Otra forma equivalente de leer Parquet, usando el método .format()
df6 = spark.read.format("parquet").load("./data/dataPARQUET.parquet")
df6.show()
df6.printSchema()  # Muestra la estructura del DataFrame

