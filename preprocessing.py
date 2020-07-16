from __future__ import print_function

import math
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("USAGE: project <file> <dirout>", file=sys.stderr)
		sys.exit(-1)
	# inicia spark	
	spark = SparkSession.builder.appName("Project").getOrCreate()

	#leer datos
	dataRDD = spark.read.text(sys.argv[1]).rdd.map(lambda r: (r[0].split(",")[0], r[0].split(",")[1:]))
	labelsRDD = dataRDD.flatMap(lambda (k,v): [k])

	# obtenemos los dias de registro en formato 'YYYY-MM-DD'
	labels = labelsRDD.collect()[15:]

	# rotar datos
	mapRDDT1 = dataRDD.values().zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for j,e in enumerate(x)])
	mapRDDT2 = mapRDDT1.map(lambda (i,j,x): (j, (i,x))).groupByKey()
	mapRDDT3 = mapRDDT2.map(lambda (i,x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
	mapRDDT4 = mapRDDT3.map(lambda x: map(lambda (i,e): e, x))

	# filtramos todas las estaciones pertenecientes a GHCN
	precipRDD1 = mapRDDT4.filter(lambda x: x[1] != "GHCN")

	# filtramos todas las estaciones fuera del chile continentan, por simplicidad se asume como latitud [-55, -18], longitud [-76, -66]
	precipRDD2 = precipRDD1.filter(lambda x:  (float(x[6]) > -76.0) and (float(x[6]) < -66.0) and (float(x[5]) > -55.0) and (float(x[5]) < -18.0))
	# (x[5] > -55) and (x[5] < -18)


	# realizamos un flatmap para los datos de la tabla rotada ignorando las  columnas 2-15 correspondientes a informacion de las estaciones
	# solo se conserva la columna 1 que contiene el codigo de la estacion, el cual es transformado a entero y luego a string
	# esto es debido a que saben que las estaciones que quedaron luego del filtro anterior poseen un codigo numerico
	# de igual forma creamos una funcion segura
	def fix_code(code):
		if code.isdigit():
			return str(int(code))
		else:
			return code

	# este flatmap collapsara los cientos de columnas (una por cada dia registra) a 4 columnas [year, month, day, value]
	precipRDD3 = precipRDD2.flatMap(lambda x: [[fix_code(x[0])] + labels[i].split("-") + [j] for i, j in enumerate(x[15:])])
	
	# filtramos datos borrando los -9999 y -9999.000
	precipRDD4 = precipRDD3.filter(lambda x: int(float(x[4])) != -9999)

	# creamos el formato para guardar en archivo
	out_text = precipRDD4.map(lambda x: "\t".join(x[:-1]) + "\t" + str(x[-1]))
	

	out_text.coalesce(1).saveAsTextFile(sys.argv[2])
	spark.stop()
