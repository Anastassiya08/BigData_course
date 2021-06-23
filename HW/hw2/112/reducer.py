#!/usr/bin/env python3

import sys

current_key = None
sum_count = 0
for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
	        print(current_key, sum_count, sep='\t')        
        sum_count = 0
        current_key = key
    sum_count += count

