from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    	spark = SparkSession.builder.appName('task2_app').master('yarn').getOrCreate()

	schema = StructType(fields=[
	    StructField("user", IntegerType()),
	    StructField("follower", IntegerType())
	])
	
	edges = spark.read.format("csv")\
          .schema(schema)\
          .option("sep", "\t")\
          .load("/data/twitter/twitter_sample.txt")

	used = edges.where("follower == 12").select("follower").limit(1)
	parents = edges.where("follower == 12")
	vertices = edges.where("follower == 12").select(col("user").alias("follower"))
	parents = parents.union(edges.join(vertices, on='follower').select("user", "follower")).distinct().cache()
	
	while True:
		used = used.union(vertices).distinct().cache()
		l = used.select('follower').rdd.map(lambda row : row[0]).collect()
		active_edges = edges.join(vertices, on='follower').cache()
		parents = parents.union(active_edges.select("user", "follower").filter(active_edges["user"].isin(l) == False)).distinct().cache() 
		vertices = active_edges.select(active_edges['user'].alias('follower')).subtract(used).cache()
		is_final = vertices.filter(vertices.follower.isin(34)).count()
		if (is_final > 0) or (vertices.count() <= 0) :
		    	break
	path = []
	v = 34
	while v != 12:
		path.append(v)
		v = parents.filter(parents.user.isin(v)).first().follower
	path.append(12)	
	
	path.reverse()
	answer = ','.join(map(str, path))
	print('%s' % (answer))


