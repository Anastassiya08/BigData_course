#!/bin/bash

host="$(hdfs fsck -blockId $1 | grep -E -o 'hadoop2-[0-9]{1,2}\.yandex.ru' | head -1)"
loc="$(ssh $host find /u*/hdfs -name $1)"
echo "$host:$loc"

