#!/bin/bash

hdfs fsck $1 -files -blocks -locations | grep -c "BP-"

