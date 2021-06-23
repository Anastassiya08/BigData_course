#!/usr/bin/env python3

import sys
import io
import re

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

stop_words = set()

for line in sys.stdin:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    text = re.sub(r'[^A-Za-z\\s\s]','',text).lower()        
    words = set(re.split("\W*\s+\W*", text, flags=re.UNICODE))

    for word in words:
        print(word, 1, sep='\t')





