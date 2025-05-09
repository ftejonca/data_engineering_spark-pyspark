"""
EJERCICIOS
1 - Cree un RDD importes a partir de los datos adjuntos a esta lección como recurso. 
    Emplee acumuladores para obtener el total de ventas realizadas y el importe total de las ventas.
2 - Si se conoce que a cada venta hay que restarle un importe fijo igual a 10 pesos por temas de impuestos.
    a - ¿Cómo restaría este impuesto de cada venta utilizando una variable broadcast para acelerar el proceso?
    b - Cree un RDD llamado ventas_sin_impuestos a partir de la propuesta del inciso a que contenga las ventas sin impuestos.
    c - Destruya la variable broadcast creada luego de emplearla para crear el RDD del inciso b.
3 - Persista el RDD ventas_sin_impuestos en los siguientes niveles de persistencia.
    a - Memoria.
    b - Disco solamente
    c - Memoria y disco.
"""

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# 1 -
importes = sc.textFile("./data/rdd.txt")
importes.take(5)

acumulador = sc.accumulator(0)
acumulador1 = sc.accumulator(0)

importes.foreach(lambda x: acumulador.add(1))
importes.foreach(lambda x: acumulador1.add(float(x)))

print(acumulador.value)
print(acumulador1.value)

# 2.a.b -

impuesto = sc.broadcast(10)

ventas_sin_impuestos = importes.map(lambda x: float(x) - impuesto.value)
ventas_sin_impuestos.take(5)

# 2c - 
impuesto.destroy()

# 3 - 
from pyspark.storagelevel import StorageLevel

# 3a - 
ventas_sin_impuestos.persist(StorageLevel.MEMORY_ONLY)

# 3b -
ventas_sin_impuestos.unpersist()
ventas_sin_impuestos.persist(StorageLevel.DISK_ONLY)

# 3c -
ventas_sin_impuestos.unpersist()
ventas_sin_impuestos.cache()# 
