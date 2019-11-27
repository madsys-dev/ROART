#!/bin/bash

for((type=0;type<2;type++))
do
    for((kt=0;kt<2;kt++))
    do
        for((bench=0;bench<2;bench++))
        do
            for tnum in 1 2 4 8 16 18 20 22 24
            do
                ../build/benchmark -t ${type} -K ${kt} -b ${bench} -n ${tnum}
            done
        done
    done
done