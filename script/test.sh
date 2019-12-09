#!/bin/bash

#LD_PRELOAD="/usr/local/lib/libvmmalloc.so.1" ../build/benchmark -t ${type} -K 1 -b ${bench} -n ${tnum}

echo "test microbench"
bash test_microbench.sh

echo "test skewness"
bash test_skew.sh

echo "test rr scalability"
bash test_scalability.sh

echo "test ycsb"
bash test_ycsb.sh