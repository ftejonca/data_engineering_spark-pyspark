### Acciones RDD ###

# Función reduce()

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# Se crea un RDD con los elementos [2, 4, 6, 8]
rdd = sc.parallelize([2, 4, 6, 8])

# Se aplica 'reduce' para sumar todos los elementos del RDD.
# Resultado: 2 + 4 + 6 + 8 = 20
rdd.reduce(lambda x, y: x + y)

# Otro RDD con los elementos [1, 2, 3, 4]
rdd1 = sc.parallelize([1, 2, 3, 4])

# Se aplica 'reduce' para multiplicar todos los elementos.
# Resultado: 1 * 2 * 3 * 4 = 24
rdd1.reduce(lambda x, y: x * y)

# --------------------------------------

# Función count()

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Se crea un RDD con los caracteres de la palabra "jose"
rdd = sc.parallelize(["j", "o", "s", "e"])

# 'count' devuelve la cantidad de elementos en el RDD.
# Resultado: 4
rdd.count()

# Se crea un RDD con los números del 0 al 9
rdd1 = sc.parallelize([item for item in range(10)])

# Se cuenta la cantidad de elementos del RDD.
# Resultado: 10
rdd1.count()

# --------------------------------------

# Función collect()

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Se crea un RDD dividiendo el string en palabras
rdd = sc.parallelize("Hola Apache Spark!".split(" "))

# 'collect' devuelve todos los elementos del RDD como una lista.
# Resultado: ['Hola', 'Apache', 'Spark!']
rdd.collect()

# Se crea un RDD con tuplas (número, cuadrado del número)
rdd1 = sc.parallelize([(item, item ** 2) for item in range(20)])

# Se recoge el contenido del RDD como lista
rdd1.collect()

# --------------------------------------

# Funciones take(), max() y saveAsTextFile()

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Se crea un RDD a partir de las palabras en la frase dada
rdd = sc.parallelize("Pour ceux qui viendront après".split(" "))

# 'take' devuelve los primeros n elementos del RDD (en este caso, 2).
# Resultado: ['Pour', 'ceux']
rdd.take(2)

# Se crea un RDD con valores tipo: n / (n+1) para n en 0 a 9
rdd1 = sc.parallelize([item/(item + 1) for item in range(10)])

# 'max' devuelve el valor máximo del RDD
# Resultado: el valor más alto entre item / (item+1)
rdd1.max()

# Se recogen todos los elementos del RDD como lista
rdd1.collect()

# Se recogen todos los elementos del RDD original (palabras de la frase)
rdd.collect()

# Se guarda el contenido del RDD en una carpeta llamada 'rdd1'
rdd.saveAsTextFile("./rdd1")

# Se guarda el RDD en un único archivo (coalesce(1) reduce el número de particiones a 1)
rdd.coalesce(1).saveAsTextFile("./rdd2")