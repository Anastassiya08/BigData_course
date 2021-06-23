from pyspark import SparkContext, SparkConf

def parse_edge(s):
	user, follower = s.split("\t")
	return (int(follower), int(user))

def step(item):
	next_v, dummy = item[1][0], item[1][1]
	return (next_v, dummy)

if __name__ == "__main__":
	config = SparkConf().setAppName("task1_app").setMaster("yarn")
	sc = SparkContext(conf=config)

	n = 400 
	edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge).partitionBy(n).cache()

	vertices = sc.parallelize([(12, 1)])
	parents = sc.parallelize([])

	while True:
		parents = parents.union(edges.join(vertices, n).map(lambda i: (i[0], i[1][0])).distinct()).cache()
		vertices = edges.join(vertices, n).map(step).cache()
		is_final = vertices.filter(lambda x: x[0]==34).isEmpty()
		if (vertices.isEmpty()) or (not is_final):
			break
		
	path = []
	v = 34
	while v != 12:
		path.append(v)
		v = parents.filter(lambda x: x[1] == v).first()[0]
	path.append(12)
	path.reverse()
	answer = ','.join(map(str, path))
	print('%s' % (answer))
