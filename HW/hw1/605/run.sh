#!/bin/bash

dd if=/dev/zero of=ex.txt  bs=$1  count=1
hdfs dfs -put ex.txt /data
sizes=$(hdfs fsck /data/ex.txt  -files -blocks | grep -E -o -w "len=\w+" | sed 's|.*=||')
hdfs dfs -rm /data/ex.txt

for i in $sizes
do
total=$(($total+$i))
done
diff=$(($total-$1))
echo $diff

