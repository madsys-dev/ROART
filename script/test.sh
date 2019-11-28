#!/bin/bash

for((type=1;type<2;type++))
do
    for((kt=0;kt<2;kt++))
    do
        for bench in 0 1 3
        do
            for tnum in 1 2 4 8 16
            do
                LD_PRELOAD="/usr/local/lib/libvmmalloc.so.1" ../build/benchmark -t ${type} -K ${kt} -b ${bench} -n ${tnum}
            done
        done
    done
done