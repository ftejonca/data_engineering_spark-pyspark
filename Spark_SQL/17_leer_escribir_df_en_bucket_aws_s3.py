### Leer y escribir un dataframe en un bucket de AWS S3 ###

# Instalar SDK java 8

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Descargar Spark

!wget -q https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz

# Descomprimir la versión de Spark

!tar -xf spark-3.3.4-bin-hadoop3.tgz

# Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.4-bin-hadoop3"

#Descargar findspark

!pip install -q findspark

# Instalar dotenv para manejar las credenciales

!pip install python-dotenv

# Extraer las credenciales del archivo .env a un diccionario de Python

from dotenv import dotenv_values
config = dotenv_values(".env")

# Crear la sesión de Spark con las configuraciones necesarias para conectarse a AWS S3

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.469")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
         )

# Extraer las credenciales del diccionario

accessKeyId=config.get("ACCES_KEY")
secretAccessKey=config.get("SECRET_ACCES_KEY")

# Establecer las configuraciones necesarias de Hadoop

sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', accessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')

# Lee un archivo Parquet desde el bucket S3 llamado 'ftejoncadev', dentro de la carpeta 'parquet'
df = spark.read.parquet("s3a://ftejoncadev/parquet")
df.show()  # Muestra las primeras filas

# Lee un archivo CSV desde el mismo bucket, infiriendo el esquema y usando la primera fila como cabecera
df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://ftejoncadev/csv")
df1.show()

# Escribe el DataFrame df nuevamente en formato Parquet en la ruta 's3a://ftejoncadev/salida', sobrescribiendo si ya existe
df.write.mode("overwrite").parquet("s3a://ftejoncadev/salida")