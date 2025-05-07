"""
EJERCICIOS
1 - Cree un RDD llamado lenguajes que contenga los siguientes lenguajes de programación: Python, R, C, Scala, Rugby y SQL.
    a - Obtenga un nuevo RDD a partir del RDD lenguajes donde todos los lenguajes de programación estén en mayúsculas.
    b - Obtenga un nuevo RDD a partir del RDD lenguajes donde todos los lenguajes de programación estén en minúsculas.
    c - Cree un nuevo RDD que solo contenga aquellos lenguajes de programación que comiencen con la letra R.
2 - Cree un RDD llamado pares que contenga los números pares existentes en el intervalo [20;30].
    a - Cree el RDD llamado sqrt, este debe contener la raíz cuadrada de los elementos que componen el RDD pares.
    b - Obtenga una lista compuesta por los números pares en el intervalo [20;30] y sus respectivas raíces cuadradas. 
    Un ejemplo del resultado deseado para el intervalo [50;60] sería la lista 
    [50, 7.0710678118654755, 52, 7.211102550927978, 54, 7.3484692283495345, 56, 7.483314773547883, 58, 7.615773105863909, 60, 7.745966692414834].
    c - Eleve el número de particiones del RDD sqrt a 20.
    d - Si tuviera que disminuir el número de particiones luego de haberlo establecido en 20, ¿qué función utilizaría para hacer más eficiente su código?
3 - Cree un RDD del tipo clave valor a partir de los datos adjuntos como recurso a esta lección. 
    Tenga en cuenta que deberá procesar el RDD leído para obtener el resultado solicitado. 
    Supongamos que el RDD resultante de tipo clave valor refleja las transacciones realizadas por número de cuentas. 
    Obtenga el monto total por cada cuenta.
"""

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# 1 - 
lenguajes = sc.parallelize(["Python", "R", "C", "Scala", "Ruby", "SQL"])
lenguajes.collect()

# 1.a - 
lenguajes_mayus = lenguajes.map(lambda x: x.upper())
lenguajes_mayus.collect()

# 1.b -
lenguajes_minus = lenguajes.map(lambda x: x.lower())
lenguajes_minus.collect()

# 1.c - 
lenguajes_r = lenguajes.filter(lambda x: x.startswith("R"))
lenguajes_r.collect()

# 2 -
rdd = sc.parallelize(range(20, 31))
rdd_pares = rdd.filter(lambda x: x % 2 == 0)
rdd_pares.collect()

# 2.a - 
import math
sqrt = rdd_pares.map(lambda x: math.sqrt(x))
sqrt.collect()

sqrt2 = rdd_pares.map(lambda x: x ** 0.5)
sqrt2.collect()

# 2.b - 
lista = rdd_pares.flatMap(lambda x: (x, math.sqrt(x)))
lista.collect()

# 2.c - 
sqrt20 = sqrt.repartition(20)
sqrt20.getNumPartitions()

# 2.d -
sqrt5 = sqrt20.coalesce(5)
sqrt5.getNumPartitions()

# 3 - 
rdd_text = sc.textFile("./data/")
rdd_text.collect()

def proceso(s):
  return (tuple(s.replace("(", "").replace(")", "").split(", ")))

rdd_llave_valor = rdd_text.map(proceso)
rdd_llave_valor.collect()

rdd_llave_valor.reduceByKey(lambda x, y: float(x) + float(y)).collect()