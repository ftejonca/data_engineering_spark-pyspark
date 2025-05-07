import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

### Función map() ###

# Creamos un RDD a partir de una lista de números
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Usamos map() para restar 1 a cada número del RDD
rdd_resta = rdd.map(lambda x: x - 1)
rdd_resta.collect()  # Resultado: [0, 1, 2, 3, 4]

# Usamos map() para verificar si cada número es par
rdd_par = rdd.map(lambda x: x % 2 == 0)
rdd_par.collect()  # Resultado: [False, True, False, True, False]

# Creamos un RDD con nombres
rdd_texto = sc.parallelize(["Fernando", "Yai", "Ari", "Eizan", "Ilian"])

# Convertimos cada nombre a mayúsculas
rdd_mayuscula = rdd_texto.map(lambda x: x.upper())
rdd_mayuscula.collect()  # Resultado: ["FERNANDO", "YAI", "ARI", "EIZAN", "ILIAN"]

# Agregamos "Hola " al inicio de cada nombre
rdd_hola = rdd_texto.map(lambda x: "Hola " + x)
rdd_hola.collect()  # Resultado: ["Hola Fernando", "Hola Yai", ...]

### Función flatMap() ###

rdd = sc.parallelize([1, 2, 3, 4, 5])

# map() devuelve un par (número, cuadrado)
rdd_cuadrado = rdd.map(lambda x: (x, x ** 2))
rdd_cuadrado.collect()  # Resultado: [(1, 1), (2, 4), ...]

# flatMap() devuelve los elementos "plana" en vez de tuplas
rdd_cuadrado_flat = rdd.flatMap(lambda x: (x, x ** 2))
rdd_cuadrado_flat.collect()  # Resultado: [1, 1, 2, 4, 3, 9, ...]

# Con flatMap, devolvemos cada nombre seguido de su versión en mayúsculas
rdd_texto = sc.parallelize(["Fernando", "Yai", "Ari", "Eizan", "Ilian"])
rdd_mayuscula = rdd_texto.flatMap(lambda x: (x, x.upper()))
rdd_mayuscula.collect()
# Resultado: ["Fernando", "FERNANDO", "Yai", "YAI", ...]

### Función filter() ###

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

# Filtramos los pares
rdd_pares = rdd.filter(lambda x: x % 2 == 0)
rdd_pares.collect()  # Resultado: [2, 4, 6, 8]

# Filtramos los impares
rdd_impares = rdd.filter(lambda x: x % 2 != 0)
rdd_impares.collect()  # Resultado: [1, 3, 5, 7, 9]

# Creamos un RDD con nombres
rdd_texto = sc.parallelize(["Fernando", "Yai", "Ari", "Eizan", "Ilian"])

# Filtramos los nombres que empiezan con "Y"
rdd_y = rdd_texto.filter(lambda x: x.startswith("Y"))
rdd_y.collect()  # Resultado: ["Yai"]

# Filtramos los que empiezan por "A" y tienen la "r" en la segunda posición (índice 1)
rdd_filtro = rdd_texto.filter(lambda x: x.startswith("A") and x.find("r") == 1)
rdd_filtro.collect()  # Resultado: ["Ari"]

### Función coalesce() ###

# Creamos un RDD con 10 particiones
rdd = sc.parallelize([1, 2, 3.4, 5], 10)
rdd.getNumPartitions()  # Resultado: 10

# coalesce() reduce el número de particiones, pero no modifica el RDD original
rdd.coalesce(5)
rdd.getNumPartitions()  # Sigue siendo 10

# Para que funcione, hay que guardar el resultado en una nueva variable
rdd5 = rdd.coalesce(5)
rdd5.getNumPartitions()  # Resultado: 5

### Función repartition() ###

# Creamos un RDD con 3 particiones
rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
rdd.getNumPartitions()  # Resultado: 3

# repartition() puede aumentar o disminuir el número de particiones y hace un shuffle
rdd7 = rdd.repartition(7)
rdd7.getNumPartitions()  # Resultado: 7

### Función reduceByKey() ###

# Creamos un RDD de pares clave-valor
rdd = sc.parallelize([
    ("casa", 2),
    ("parque", 1),
    ("que", 5),
    ("casa", 1),
    ("escuela", 2),
    ("casa", 1),
    ("que", 1)
])
rdd.collect()

# Agrupamos por clave y sumamos los valores (reduceByKey)
rdd_reducido = rdd.reduceByKey(lambda x, y: x + y)
rdd_reducido.collect()
# Resultado: [("casa", 4), ("parque", 1), ("que", 6), ("escuela", 2)]