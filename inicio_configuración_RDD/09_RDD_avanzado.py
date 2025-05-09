### Aspectos avanzados sobre RDD ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# -------------------------------
# Almacenamiento en caché

from pyspark.storagelevel import StorageLevel

# Se crea un RDD con los números del 0 al 9
rdd = sc.parallelize([item for item in range(10)])

# Se persiste el RDD en la memoria (RAM) únicamente
rdd.persist(StorageLevel.MEMORY_ONLY)

# Se libera el RDD de la memoria
rdd.unpersist()

# Se persiste el RDD solamente en disco (no en memoria)
rdd.persist(StorageLevel.DISK_ONLY)

# Se libera nuevamente el RDD
rdd.unpersist()

# Se almacena el RDD en memoria utilizando 'cache', equivalente a persist(MEMORY_AND_DISK) por defecto
rdd.cache()


# -------------------------------
# Particionamiento (HashPartitioner simulado)

# Se crea un RDD con los elementos "x", "y", "z"
rdd = sc.parallelize(["x", "y", "z"])

# Se define una cadena
hola = "Hola"

# Se calcula el hash de la cadena 'hola'
hash(hola)

# Se define el número de particiones
num_particiones = 6

# Se simula cómo se determinaría la partición de cada elemento usando hash % número de particiones
hash("x") % num_particiones
hash("y") % num_particiones
hash("z") % num_particiones

# Esto ilustra cómo Spark distribuye datos entre particiones usando funciones hash


# -------------------------------
# Variables Broadcast

# Se crea un RDD con los números del 0 al 9
rdd = sc.parallelize([item for item in range(10)])
rdd.collect()  # Se muestra el contenido del RDD

# Se define una variable y se crea una variable broadcast para compartirla eficientemente entre nodos
uno = 1
br_uno = sc.broadcast(uno)

# Se usa la variable broadcast en una transformación para sumar 1 a cada elemento
rdd1 = rdd.map(lambda x: x + br_uno.value)
rdd1.collect()

# Se despersistiza la variable broadcast (se libera de la caché de los ejecutores)
br_uno.unpersist()

# Aún se puede usar la variable broadcast después de unpersist, mientras no se destruya
rdd1 = rdd.map(lambda x: x + br_uno.value)
rdd1.collect()

# Se destruye completamente la variable broadcast
br_uno.destroy()

# Intentar usarla tras destruirla puede provocar error (aunque no se gestiona aquí)
rdd1 = rdd.map(lambda x: x + br_uno.value)
rdd1.take(5)


# -------------------------------
# Acumuladores

# Se crea un acumulador inicializado en 0
acumulador = sc.accumulator(0)

# Se utiliza 'foreach' para sumar cada elemento del RDD al acumulador
rdd.foreach(lambda x: acumulador.add(x))

# Se imprime el valor total acumulado (suma de 0 a 9 = 45)
print(acumulador.value)

# Se crea otro RDD a partir de palabras de una frase
rdd1 = sc.parallelize("Mi nombre es Fernando Tejón y me siento genial".split(" "))

# Acumulador para contar cuántas palabras hay (una por cada ejecución de 'foreach')
acumulador1 = sc.accumulator(0)

# Se suma 1 por cada palabra recorrida
rdd1.foreach(lambda x: acumulador1.add(1))

# Se imprime el total de palabras (debería ser 9)
print(acumulador1.value)