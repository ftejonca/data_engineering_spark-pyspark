"""
Todos los pasos que a continuación se detallan,
son para utilizar spark en google colaboratory.
Son los pasos para instalar lo necesario para usar spark desde google colaboratory.
"""


# 1- Instalar SDK Java 8

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# 2 - Descargar Spark 3.5.5 -  desde la web https://archive.apache.org/dist/spark/  se pueden ver las versiones de spark

!wget -q https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.5.5-bin-hadoop3.tgz

# 3 - Descomprimir el archivo descargado de Spark

!tar xf spark-3.5.5-bin-hadoop3.tgz 

# 4 - Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.5-bin-hadoop3"

# 5 - nstalar la librería findspark 

!pip install -q findspark

# 6 - Instalar pyspark

!pip install -q pyspark

# 7 - verificar la instalación

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

# 8 - Probar la sesión de Spark
df = spark.createDataFrame([{"Hola": "Mundo"} for x in range(10)])

df.show(10, False)

""" IMPORTANTE!!! todos estos pasos, se han de repetir cada vez que se comience a trabajar con google colab
puesto que, las variables de entorno y las instalaciones no se guardan de una sesión a otra """