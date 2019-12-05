#!/bin/bash

#LD_PRELOAD="/usr/local/lib/libvmmalloc.so.1" ../build/benchmark -t ${type} -K 1 -b ${bench} -n ${tnum}
for((type=0;type<1;type++))
do
    for((kt=1;kt<2;kt++))
    do
        for bench in 5 6
        do
            for tnum in 1 2 4 8 16 32
            do
                # scalability uniform and zipfian
                ../build/benchmark -t 0 -K 1 -b ${bench} -n ${tnum} >> ../result/art_workload_uni.res
                ../build/benchmark -t 0 -K 1 -b ${bench} -n ${tnum} -w 1 -S 0.8 >> ../result/art_workload_zip.res

                ../build/benchmark -t 1 -K 1 -b ${bench} -n ${tnum} >> ../result/ff_workload_uni.res
                ../build/benchmark -t 1 -K 1 -b ${bench} -n ${tnum} -w 1 -S 0.8 >> ../result/ff_workload_zip.res

            done
        done

        for skewness in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.99
        do
            # skewness
            ../build/benchmark -t 0 -K 1 -b 5 -n 12 -w 1 -S ${skewness} >> ../result/12_5_skewness.res
            ../build/benchmark -t 1 -K 1 -b 5 -n 12 -w 1 -S ${skewness} >> ../result/12_5_skewness.res
        done
    done
done