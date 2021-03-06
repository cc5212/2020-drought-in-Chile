from __future__ import print_function

import math
import sys
from pyspark.sql import SparkSession

def fix_code(code):
		if code.isdigit():
			return str(int(code))
		else:
			return code

def median(lst):
	n = len(lst)
	s = sorted(lst)
	return (sum(s[n//2-1:n//2+1])/2.0, s[n//2])[n % 2] if n else None

def mean(lst):
	return sum(lst) / len(lst)

def variance(lst):
	m = mean(lst)
	return sum(pow(x-m,2) for x in lst) / len(lst)

def std(lst):
	return math.sqrt(variance(lst))

def norm(x, mu, std):
	return (x-mu) / std


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("USAGE: project <file1> <file2> <dirout>", file=sys.stderr)
		sys.exit(-1)
	# inicia spark	
	spark = SparkSession.builder.appName("Project").getOrCreate()

	# data cols are: code##year		days_sum	yearly_precip
	dataRDD = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0].split("\t"))
	dataRDD2 = dataRDD.map(lambda x: [ fix_code(x[0].split("##")[0]), x[0].split("##")[1], x[2] ] )

	# print(dataRDD2.take(5))

	# file with spatial division, cols: code	zone_number
	divisionRDD = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0].split(","))

	zones_arr = divisionRDD.collect()
	zones_dict = {}
	for x in zones_arr:
		zones_dict[fix_code(x[0])] = int(x[1])

	# print(zones_dict)


	databyZoneRDD = dataRDD2.map(lambda x: [x[0], x[1], x[2], zones_dict[x[0]]])

	dataByZoneString = databyZoneRDD.map(lambda x: (x[0] + "\t" + x[1] + "\t" + str(x[2]) + "\t" + str(x[3])))


	# obtenemos el valor medio creando una llave zone+year
	dataMedianRDD = databyZoneRDD.map(lambda x: (str(x[3]) + "##" + x[1],  [float(x[2])])).reduceByKey(lambda a,b: a + b).map(lambda x: (x[0], median(x[1])))

	# print(dataMedianRDD.take(20))

	dataMedianString = dataMedianRDD.map(lambda x: (x[0].split("##")[0] + "\t" + x[0].split("##")[1] + "\t" + str(x[1]) ))

	# obtenemos un vector con todos los valores por zona y calculamos mean and std
	dataZoneMeanStdRDD = databyZoneRDD.map(lambda x: (str(x[3]), [float(x[2])])).reduceByKey(lambda a,b: a+b).map(lambda x: [x[0], mean(x[1]), std(x[1])])

	# print(dataZoneMeanStdRDD.take(20))

	dataZoneMeanStdString = dataZoneMeanStdRDD.map(lambda x: (x[0] + "\t" + str(x[1]) + "\t" + str(x[2])))


	# the input in sys.argv[3] has to exists
	dataMedianString.coalesce(1).saveAsTextFile(sys.argv[3] + "median-year-zone-yearly-data/")
	dataZoneMeanStdString.coalesce(1).saveAsTextFile(sys.argv[3] + "mean-std-zone-yearly-data/")
	dataByZoneString.coalesce(1).saveAsTextFile(sys.argv[3] + "yearly-data-with-zone/")


	# generemos la tabla con los SPI por estacion por zona
	dataMedianRDD2 = dataMedianRDD.map(lambda x: (x[0].split("##")[0], (x[0].split("##")[1], x[1])))
	dataZoneMeanStdRDD2 = dataZoneMeanStdRDD.map(lambda x: (x[0], (x[1], x[2])))

	dataSPIString = dataMedianRDD2.join(dataZoneMeanStdRDD2).map(lambda x: (str(x[0]) + "\t" + str(x[1][0][0]) + "\t" + str(norm(x[1][0][1], x[1][1][0], x[1][1][1]))))

	# print(dataSPIString.take(20))

	dataSPIString.coalesce(1).saveAsTextFile(sys.argv[3] + "yearly-SPI-by-zone/")


	# ahora calculamos precipitacion promedio anual por cada estacion
	meanYearlyStationRDD = dataRDD2.map(lambda x: (x[0], [float(x[2])])).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], mean(x[1])))

	# obtenemos code, latitude y longitud de cr2_prDaily_2018_stations_ghcn.txt y le agregamos la division por zonas
	dataStationsRDD = spark.read.text(sys.argv[3] + "cr2_prDaily_2018_stations_ghcn.txt").rdd.map(lambda r: r[0].split(",")).map(lambda r: (r[0], (r[5], r[6], zones_dict.get(r[0], 0))))


	# unimos ambas tablas
	dataMeanLatLonRDD = meanYearlyStationRDD.join(dataStationsRDD)


	# print(dataMeanLatLonRDD.take(20))

	# generamos el string de salida
	dataMeanLatLonString = dataMeanLatLonRDD.map(lambda x: (x[0] + "\t" + str(x[1][0]) + "\t" + x[1][1][0] + "\t" + x[1][1][1] + "\t" + str(x[1][1][2])))

	dataMeanLatLonString.coalesce(1).saveAsTextFile(sys.argv[3] + "mean-yearly-precip-lat-lon-zone/")


	spark.stop()