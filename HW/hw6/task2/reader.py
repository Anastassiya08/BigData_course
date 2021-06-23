#!/usr/bin/env python3

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--tag')
parser.add_argument('--start_id')
parser.add_argument('--end_id')
args = parser.parse_args()

cluster = Cluster(['mipt-node01', 'mipt-node02', 'mipt-node03', 'mipt-node04', 'mipt-node05''mipt-node06', 'mipt-node07', 'mipt-node08'], load_balancing_policy = RoundRobinPolicy())
session = cluster.connect('msb202011keyspace')

resultset = session.execute("SELECT * FROM videos_metadata_hob202011 WHERE tag = %s AND user_id >= %s AND user_id <= %s ALLOW FILTERING", (args.tag, args.start_id, args.end_id))

for row in resultset:
    print(row)
