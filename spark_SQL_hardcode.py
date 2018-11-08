### Ambiente para Maquina de la oficina
import os
import sys
os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.6-src.zip")
###############################################################################

from pyspark import SparkContext, SQLContext

sc = SparkContext()
sql_sc = SQLContext(sc)
rdd = sc.textFile("alumnos_nombres.csv")
rdd_ap = sc.textFile("alumnos_apellidos.csv")



name_df_csv = rdd.map(lambda x: x.split(",")).map(lambda (id, nombre): (id, nombre, len(nombre))).toDF(["id", "nombre", "long"])

name_df_csv.show()

name_df_csv.filter(name_df_csv.long.between(4, 5)).show()

ln_df_csv = rdd_ap.map(lambda x: x.split(",")).map(lambda (id,apellido): (id, apellido, len(apellido))).toDF(["id", "apellido", "long"])

ln_df_csv.show()

ln_df_csv.filter(ln_df_csv.long.between(7, 8)).show()


name_df_csv.createTempView("nombres")
ln_df_csv.createTempView("apellidos")

sql_sc.sql("SELECT n.nombre, a.apellido, n.long+a.long lt FROM nombres n JOIN apellidos a ON n.id = a.id").show()

#inner_join = ta.join(tb, ta.name == tb.name)
#inner_join.show()

inner_join = name_df_csv.join(ln_df_csv, name_df_csv.id == ln_df_csv.id)
inner_join.show()



#dfcsv

#sql_sc.sql("SELECT * FROM nombres n JOIN apellidos a ON n.id = a.id").show()

print("Ok")