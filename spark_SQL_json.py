### Ambiente para Maquina de la oficina
import os
import sys
os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.6-src.zip")
###############################################################################
from pyspark.sql.types import *
from pyspark.sql.functions import length


from pyspark.sql import SparkSession

from pyspark import SparkContext, SQLContext


sc = SparkContext()
sqlc = SQLContext(sc)


schema_names = StructType([
    StructField("id", StringType(), False),
    StructField("nombre", StringType(), False)
])

schema_ln = StructType([
    StructField("id", StringType(), False),
    StructField("apellido", StringType(), False)
])

name_df_json = sqlc.read.json('alumnos_nombres.json', multiLine=True, schema=schema_names)
ln_df_json = sqlc.read.json('alumnos_apellidos.json', multiLine=True, schema=schema_ln)

name_df_json_long = name_df_json.withColumn("long", length("nombre"))
ln_df_json_long = ln_df_json.withColumn("long", length("apellido"))


name_df_json_long.show()

ln_df_json_long.show()

name_df_json_long.createTempView("nombres")
ln_df_json_long.createTempView("apellidos")

sqlc.sql("SELECT n.nombre, a.apellido, n.long+a.long lt  FROM nombres n JOIN apellidos a ON n.id = a.id").show()

inner_join = name_df_json_long.join(ln_df_json_long, name_df_json_long.id == ln_df_json.id)
inner_join.show()



print("Ok")
