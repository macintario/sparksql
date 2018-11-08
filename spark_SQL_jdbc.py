### Ambiente para Maquina de la oficina
import os
import sys
os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.6-src.zip")
###############################################################################

from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import length

sc = SparkContext()
sql_sc = SQLContext(sc)



spark = SparkSession \
        .builder \
        .appName("Practica SparkSQL") \
        .getOrCreate()


name_df_sql = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/sparksql?user=root&password=Cic1234*")\
    .option("driver", "org.mariadb.jdbc.Driver")\
    .option("dbtable", "(select id, nombre from alumnos) as tn")\
    .load()

ln_df_sql = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/sparksql?user=root&password=Cic1234*")\
    .option("driver", "org.mariadb.jdbc.Driver")\
    .option("dbtable", "(select id, apellido from alumnos) as tn")\
    .load()

name_df_sql.show()
ln_df_sql.show()



name_df_json_long = name_df_sql.withColumn("long", length("nombre"))
ln_df_json_long = ln_df_sql.withColumn("long", length("apellido"))


name_df_json_long.show()
ln_df_json_long.show()

name_df_json_long.filter(name_df_json_long.long.between(4, 5)).show()
ln_df_json_long.filter(ln_df_json_long.long.between(7, 8)).show()

name_df_json_long.createTempView("nombres")
ln_df_json_long.createTempView("apellidos")

sql_sc.sql("SELECT n.nombre, a.apellido, n.long+a.long lt  FROM nombres n JOIN apellidos a ON n.id = a.id").show()

inner_join = name_df_json_long.join(ln_df_json_long, name_df_json_long.id == ln_df_sql.id)
inner_join.show()



print("Ok")
