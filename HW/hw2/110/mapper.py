#!/usr/bin/env python3

import sys
import random

for line in sys.stdin:
    try:
        ids = line.strip()
    except ValueError as e:
        continue
    print(random.randint(1,10), ids, sep='\t')





