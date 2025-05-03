import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Obtiene el SparkContext a partir de la SparkSession (necesario para trabajar con RDDs)
sc = spark.sparkContext

# Crea un RDD vacío sin particiones definidas
rdd_vacio = sc.emptyRDD

# Crea un RDD vacío con 3 particiones explícitamente definidas
rdd_vacio_con_3_particiones = sc.parallelize([], 3)

# Devuelve el número de particiones del RDD anterior (debería ser 3)
rdd_vacio_con_3_particiones.getNumPartitions()

# Crea un RDD a partir de una lista de números del 1 al 5
rdd = sc.parallelize([1, 2, 3 ,4 ,5])
rdd  # Muestra la referencia del RDD (no ejecuta nada aún)

# Recoge los elementos del RDD como una lista en el driver (acción)
rdd.collect()

# Crea un RDD leyendo el archivo de texto línea por línea (una línea por elemento)
rdd_texto = sc.textFile("./rdd_source.txt")

# Recoge y muestra el contenido del archivo línea a línea
rdd_texto.collect()

# Crea un RDD que lee el archivo completo como un solo bloque (clave: nombre del archivo, valor: contenido completo)
rdd_texto_completo = sc.wholeTextFiles("./rdd_source.txt")

# Recoge y muestra el contenido del RDD como pares (ruta del archivo, texto completo)
rdd_texto_completo.collect()

# Crea un nuevo RDD sumando 1 a cada elemento del RDD original (transformación con map)
rdd_suma = rdd.map(lambda x: x + 1)

# Recoge y muestra los resultados del RDD transformado
rdd_suma.collect()

# Crea un DataFrame a partir de una lista de tuplas (ID, Nombre)
df = spark.createDataFrame([
    (1, "Fernando"),
    (2, "Yaiza"),
    (3, "Ari"),
    (4, "Eizan"),
    (5, "Ilian")
], ["ID", "Nombre"])

# Muestra el contenido del DataFrame en formato tabular
df.show()

# Convierte el DataFrame en un RDD (cada fila del DataFrame se convierte en una fila del RDD)
rdd_df = df.rdd

# Recoge y muestra el contenido del RDD convertido desde el DataFrame
rdd_df.collect()

# Detiene la SparkSession y libera los recursos
spark.stop()