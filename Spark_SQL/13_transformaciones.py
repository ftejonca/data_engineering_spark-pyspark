### Tansformaciones DF ###

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Funciones select() y selectExpr()