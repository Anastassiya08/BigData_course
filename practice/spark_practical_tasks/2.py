#!/usr/bin/env python
import re
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime as dt


# regular expression for parsing log lines
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

# Parse log line, return tuple with typed values 
def parseLine(line):
    match = log_format.match(line)
    if not match:
        return ("", "", "", "", "", "", "" ,"", "")

    request = match.group('request').split()
    return (match.group('host'), match.group('time').split()[0], \
       request[0], request[1], match.group('status'), match.group('bytes'), \
        match.group('referer'), match.group('user_agent'),
        dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)


if __name__ == "__main__":
    conf = SparkConf().setAppName("503 rdd").setMaster("yarn").set("spark.ui.port", "4090")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("%s" % sys.argv[1])
    objects = lines.map(parseLine)

    ip_address = objects.filter(lambda line_tuple: len(line_tuple[0]) > 0) \
                    .map(lambda line_tuple: (line_tuple[0], line_tuple[-2], line_tuple[-1])) \
                    .distinct() \
                    .map(lambda x: (x[-1], 1)) \
                    .reduceByKey(lambda a, b: a + b) \
                    .sortByKey(True) \
                    .take(10)

    for val in ip_address:
        print '%s %s' % (val[0], val[1])
