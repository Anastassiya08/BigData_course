#!/usr/bin/env python
from __future__ import print_function

import re
import sys
from pyspark.sql import SparkSession, Row
from datetime import datetime as dt


log_format = re.compile(
    r"(?P<host>[\d\.]+)\s"
    r"(?P<identity>\S*)\s"
    r"(?P<user>\S*)\s"
    r"\[(?P<time>.*?)\]\s"
    r'"(?P<request>.*?)"\s'
    r"(?P<status>\d+)\s"
    r"(?P<bytes>\S*)\s"
    r'"(?P<referer>.*?)"\s'
    r'"(?P<user_agent>.*?)"\s*'
)


def parseLine(line):
    match = log_format.match(line)
    if not match:
        return (None, ) * 9

    request = match.group('request').split()
    return (match.group('host'), match.group('time').split()[0],
        request[0], request[1], match.group('status'), int(match.group('bytes')),
        match.group('referer'), match.group('user_agent'),
        dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)


if __name__ == "__main__":
    spark_session = SparkSession.builder.master("yarn").appName("503 df").config("spark.ui.port", "18089").getOrCreate()
    lines = spark_session.sparkContext.textFile("%s" % sys.argv[1])
    parts = lines.map(parseLine)
    rows = parts.map(lambda p: Row(ip=p[0],
                                   timestamp=p[1],
                                   request_type=p[2],
                                   request_url=p[3],
                                   status=p[4],
                                   bytes=p[5],
                                   referer=p[6],
                                   user_agent=p[7],
                                   hour=p[8]))
    access_log_df = spark_session.createDataFrame(rows)

    vals = access_log_df.dropna().drop_duplicates(["ip", "user_agent", "hour"])\
        .groupby("hour")\
        .count()\
        .sort("hour")\
        .take(10)

    for val in vals:
        print(val["hour"], val["count"], sep="\t")
