# -*- coding: utf-8 -*-
### Ambiente para Maquina de la oficina
import os
import sys


os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.6-src.zip")
###############################################################################

from pyspark import SparkContext, SQLContext
import pyspark.sql

sc = SparkContext()
sql_sc = SQLContext(sc)

losNombres = [
    u"1,Alejandro",
    u"2,Juan",
    u"3,Miguel",
    u"4,Oliver",
    u"5,Jorge",
    u"6,Jorge",
    u"7,Carlos",
    u"8,javier",
    u"9,Gamaliel",
    u"10,Eliuth",
    u"11,Ricardo",
    u"12,Montserrat",
    u"13,Eric",
    u"14,Alejandro",
    u"15,Oscar",
    u"16,Gregorio",
    u"17,Jorge",
    u"18,Deimer",
    u"19,Luis",
    u"20,Mordreaut",
    u"21,Luis",
    u"22,Geovanni"
]

losApellidos = [
    u"1,Betanzos",
    u"2,Caballero",
    u"3,Chávez",
    u"4,Cisneros",
    u"5,Espinoza",
    u"6,Farfán",
    u"7,Garnica",
    u"8,hernandez",
    u"9,Jiménez",
    u"10,Lopez",
    u"11,López",
    u"12,Mendoza",
    u"13,Montufar",
    u"14,Nava",
    u"15,Mellado",
    u"16,Rodriguez",
    u"17,Salgado",
    u"18,Sánchez",
    u"19,Torres",
    u"20,Vasquez",
    u"21,Vazquez",
    u"22,Valdivieso"
]

rdd = sc.parallelize(losNombres)
rdd_ap =sc.parallelize(losApellidos)

name_df_hc = rdd.map(lambda x: x.split(",")).map(lambda (id, nombre): (id, nombre, len(nombre))).toDF(
    ["id", "nombre", "long"])

name_df_hc.show()

name_df_hc.filter(name_df_hc.long.between(4, 5)).show()

ln_df_hc = rdd_ap.map(lambda x: x.split(",")).map(lambda (id, apellido): (id, apellido, len(apellido))).toDF(
    ["id", "apellido", "long"])

ln_df_hc.show()

ln_df_hc.filter(ln_df_hc.long.between(7, 8)).show()

name_df_hc.createTempView("nombres")
ln_df_hc.createTempView("apellidos")

sql_sc.sql("SELECT n.nombre, a.apellido, n.long+a.long lt FROM nombres n JOIN apellidos a ON n.id = a.id").show()


inner_join = name_df_hc.join(ln_df_hc, name_df_hc.id == ln_df_hc.id)
inner_join.show()

print("Ok")
