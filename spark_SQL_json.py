from pyspark.sql import SparkSession

from pyspark import SparkContext, SQLContext

#sc = SparkContext()
#sql_sc = SQLContext(sc)
#rdd = sc.textFile("alumnos_nombres.csv")

sc = SparkContext()
sqlc = SQLContext(sc)

name_df_csv= sqlc.read.json('alumnos_nombres.json', multiLine=True)
ln_df_csv = sqlc.read.json('alumnos_nombres.json', multiLine=True)

#print name_df_csv.show()


#wordcount = rdd.flatMap(lambda x: x.split()).map(lambda x: (x,len(x))).reduceByKey(lambda x, y: x ).toDF(["nombre", "long"])
#name_df_csv = rdd.map(lambda x: x.split(",")).map(lambda (id,nombre): (id, nombre, len(nombre))).toDF(["id", "nombre", "long"])

#TODO crear esquema

name_df_csv.show()

#name_df_csv.filter(name_df_csv.long.between(4, 5)).show()
#wordcount.filter(wordcount.long.between(4, 5)).show()

#wordcount_ape = rdd_ap.flatMap(lambda x: x.split()).map(lambda x: (x,len(x))).reduceByKey(lambda x, y: x ).toDF(["apellido", "long"])
#ln_df_csv = rdd_ap.map(lambda x: x.split(",")).map(lambda (id,apellido): (id, apellido, len(apellido))).toDF(["id", "apellido", "long"])

ln_df_csv.show()

#ln_df_csv.filter(ln_df_csv.long.between(7, 8)).show()


name_df_csv.createTempView("nombres")
ln_df_csv.createTempView("apellidos")

sqlc.sql("SELECT n.nombre, a.apellido FROM nombres n JOIN apellidos a ON n.id = a.id").show()

#inner_join = ta.join(tb, ta.name == tb.name)
#inner_join.show()

inner_join = name_df_csv.join(ln_df_csv, name_df_csv.id == ln_df_csv.id)
inner_join.show()



#dfcsv

#sql_sc.sql("SELECT * FROM nombres n JOIN apellidos a ON n.id = a.id").show()

print("Ok")
