#!/bin/bash

for((i=0;i<10000;i++))
do
    echo $i
    ../build/benchmark -t 1 -K 1 -b 3 -n 24 >> a.log
done