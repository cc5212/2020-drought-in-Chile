from __future__ import print_function

import math
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("USAGE: project <file> <dirout>", file=sys.stderr)
		sys.exit(-1)
	
	spark = SparkSession.builder.appName("Project").getOrCreate()

	dataRDD = spark.read.text(sys.argv[1]).rdd.map(lambda r: (r[0].split(",")[0], r[0].split(",")[1:]))
	labelsRDD = dataRDD.flatMap(lambda (k,v): [k])

	labels = labelsRDD.collect()[15:]

	mapRDDT1 = dataRDD.values().zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for j,e in enumerate(x)])



	mapRDDT2 = mapRDDT1.map(lambda (i,j,x): (j, (i,x))).groupByKey()
	mapRDDT3 = mapRDDT2.map(lambda (i,x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
	mapRDDT4 = mapRDDT3.map(lambda x: map(lambda (i,e): e, x))



	precipRDD1 = mapRDDT4.flatMap(lambda x: [[x[0]] + labels[i].split("-") + [j] for i, j in enumerate(x[15:])])
	precipRDD2 = precipRDD1.filter(lambda x: int(float(x[4])) != -9999)

	out_text = precipRDD2.map(lambda x: "\t".join(x[:4]) + "\t" + str(x[4]))
	


	out_text.coalesce(1).saveAsTextFile(sys.argv[2])
	spark.stop()
