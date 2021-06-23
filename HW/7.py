# coding: utf-8

import os
import time
from time import sleep
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext



# Preparing SparkContext
from hdfs import Config
import subprocess

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn')

# Preparing base RDD with the input data
DATA_PATH = "/data/course4/wiki/en_articles_batches"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:3]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2 # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)

finished = False
printed = False

def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True

def print_only_at_the_end(rdd):
    global printed
    #rdd.count()
    if finished and not printed:
        for word, cnt in rdd.takeOrdered(10, key=lambda x: -x[1]):
            print("{} {}".format(word, cnt))
        printed = True

def print_always(rdd):
    for word, cnt in rdd.takeOrdered(10, key=lambda x: -x[1]):
        print("{} {}".format(word, cnt))

# If we have received empty rdd, the stream is finished.
# So print the result and stop the context.

dstream.foreachRDD(set_ending_flag)

def update_func(current, old):
    if old is None:
        old = 0
    return sum(current, old)

result = dstream \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .updateStateByKey(update_func) \
    .foreachRDD(print_only_at_the_end)

ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))  # checkpoint for storing current state
ssc.start()
while not printed:
     time.sleep(0.001)
ssc.stop()
sc.stop()
