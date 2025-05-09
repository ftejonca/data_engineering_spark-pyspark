"""
EJERCICIOS
1 - Cree un RDD llamado importes a partir del archivo adjunto a esta lección como recurso.
    a - ¿Cuántos registros tiene el RDD importes?
    b - ¿Cuál es el valor mínimo y máximo del RDD importes?
    c - Cree un RDD top15 que contenga los 15 mayores valores del RDD importes. Tenga en cuenta que pueden repetirse los valores. 
    Por último, escriba el RDD top15 como archivo de texto en la carpeta data/salida.
2 - Cree una función llamada factorial que calcule el factorial de un número dado como parámetro. Utilice RDDs para el cálculo.
"""

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# 1 -
importes = sc.textFile("./data/num.txt")

# 1.a - 
importes.count()

# 1.b - 
importes.min()
importes.max()

# 1.c - 
lista_top15 = importes.top(15)
print(lista_top15)

top15 = sc.parallelize(lista_top15)
top15.saveAsTextFile("./data/salida")

# 2 - 
def factorial(n):
  if n == 0:
    return 1
  else:
    rdd = sc.parallelize(list(range(1, n + 1)))
    return rdd.reduce(lambda x,y: x * y)
  
factorial(4)