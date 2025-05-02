""" Importa la librería 'findspark', que permite inicializar Spark desde entornos como Jupyter Notebook
o desde máquinas que no tienen configurada la variable de entorno de Spark por defecto."""
import findspark

# Inicializa 'findspark' para que Python pueda localizar la instalación de Apache Spark.
findspark.init()

# Importa la clase SparkSession, que es la puerta de entrada a todas las funcionalidades de Spark SQL.
from pyspark.sql import SparkSession

""" Crea una nueva sesión de Spark o recupera una existente con SparkSession.builder.getOrCreate()
- master("local[*]") indica que se usen todos los núcleos disponibles del procesador local.
- appName("Práctica PySpark") asigna un nombre identificativo a la aplicación Spark."""
spark = SparkSession.builder.master("local[*]").appName("Práctica PySpark").getOrCreate()

"""Muestra la sesión Spark actual. Esto normalmente no imprime nada útil por sí solo,
pero en entornos como Jupyter Notebook se utiliza para confirmar que la sesión está activa."""
spark