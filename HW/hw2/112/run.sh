#!/usr/bin/env bash

OUT_DIR="streaming_wc_result"
hadoop fs -rm -r -skipTrash ${OUT_DIR} > /dev/null

NUM_REDUCERS=5

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -D mapred.job.name="my112" \
    -files mapper.py,reducer.py\
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/wiki/en_articles \
    -output ${OUT_DIR} > /dev/null

cd ${OUT_DIR}
hdfs dfs -cat part-00000 part-00001 part-00002 part-00003 part-00004 | sort -k 2nr | head -n 10

