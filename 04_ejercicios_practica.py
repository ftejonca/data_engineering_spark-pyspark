"""
EJERCICIOS
1 - Cree una sesión de Spark con nombre Cap2 y asegúrese de que emplea todos los cores disponibles para ejecutar en su ambiente de trabajo.
2 - Cree dos RDD vacíos, uno de ellos no debe contener particiones y el otro debe tener 5 particiones. Utilice vías diferentes para crear cada RDD.
3 - Cree un RDD que contenga los números primos que hay entre 1 y 20.
4 - Cree un nuevo RDD a partir del RDD creado en el ejercicio anterior el cuál solo contenga los números primos mayores a 10.
5 - Descargue el archivo de texto adjunto a esta lección como recurso y guárdelo en una carpeta llamada data en el ambiente de trabajo de Colab.
    a - Cree un RDD a partir de este archivo de texto en donde todo el documento esté contenido en un solo registro. ¿Cómo podría saber la dirección donde está guardado el archivo de texto a partir del RDD creado?
    b - Si necesitara crear un RDD a partir del archivo de texto cargado previamente en donde cada línea del archivo fuera un registro del RDD, ¿cómo lo haría?
"""

# 1. Cree una sesión de Spark con nombre Cap2 y asegúrese de que emplea todos los cores disponibles para ejecutar en su ambiente de trabajo.
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("Cap2").getOrCreate()
spark

sc = spark.sparkContext
print(sc)

# 2. Cree dos RDD vacíos, uno de ellos no debe contener particiones y el otro debe tener 5 particiones. 
# Utilice vías diferentes para crear cada RDD.
rdd_vacio_1 = sc.emptyRDD()
rdd_vacio_1.getNumPartitions()

rdd_vacio2_con_particiones = sc_2.parallelize([], 5)
rdd_vacio2_con_particiones.getNumPartitions()

# 3. Cree un RDD que contenga los números primos que hay entre 1 y 20.
rdd_numeros_primos = sc_2.parallelize([2, 3, 5, 7, 11, 13, 17, 19])
rdd_numeros_primos.collect()

# 4. Cree un nuevo RDD a partir del RDD creado en el ejercicio anterior el cuál solo contenga los números primos mayores a 10.
rdd_del_anterior = rdd_numeros_primos.filter(lambda x: x > 10)
rdd_del_anterior.collect()

# 5. Descargue el archivo de texto adjunto a esta lección como recurso y guárdelo en una carpeta llamada data en el ambiente de trabajo de Colab.
# 5.a. Cree un RDD a partir de este archivo de texto en donde todo el documento esté contenido en un solo registro. 
# ¿Cómo podría saber la dirección donde está guardado el archivo de texto a partir del RDD creado?
rdd_archivo_texto = sc_2.wholeTextFiles("./data/el_valor_del_big_data.txt")
rdd_archivo_texto.collect()

# 5.b. Si necesitara crear un RDD a partir del archivo de texto cargado previamente en donde 
# cada línea del archivo fuera un registro del RDD, ¿cómo lo haría?
rdd_archivo_texto_2 = sc_2.textFile(".data/el_valor_del_big_data.txt")
rdd_archivo_texto_2.collect()