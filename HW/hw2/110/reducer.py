#!/usr/bin/env python3

import sys
import random

s = ''
count = 0

for line in sys.stdin:
    try:
        key, ids = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    n = random.random()
    if n >= 0.5:
        if (count < 5):
            s += ids + ','
            count += 1
        else:
            print(s[:-1])
            s = ids + ','
            count = 1
    else:
        if count > 0:
            print(s[:-1])
        s = ids + ','
        count = 1
if count > 0:
    print(s[:-1])
    
