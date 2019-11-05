cmake_minimum_required(VERSION 2.8)
project(P-ART)

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE Release)
else ()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O2 -march=native -mavx -mavx2  -DNDEBUG")

## Instruction options for Cache line flush
add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
#add_definitions(-DCLWB)

link_libraries(pthread atomic boost_system boost_thread gtest)
find_library(TbbLib tbb)
include_directories(./ART
        ./benchmark
        ./nvm_mgr
        ./test
        ./fast_fair)

aux_source_directory(ART       DIR_ART_SRCS)
aux_source_directory(benchmark DIR_BENCH_SRCS)
aux_source_directory(fast_fair DIR_FF_SRCS)
aux_source_directory(nvm_mgr   DIR_NVMMGR_SRCS)
aux_source_directory(perf      DIR_PERF_SRCS)
aux_source_directory(test      DIR_TEST_SRC)

add_library(Indexes
        ./nvm_mgr/Epoche.cpp
        ./nvm_mgr/nvm_mgr.cpp
        ./nvm_mgr/threadinfo.cpp
        ./benchmark/generator.cpp
        ./ART/N4.cpp
        ./ART/N16.cpp
        ./ART/N48.cpp
        ./ART/N256.cpp
        ./ART/N.cpp
        ./ART/Tree.cpp
        )

target_link_libraries(Indexes ${TbbLib})


set(P_ART_BENCH perf/art_simple_bench.cpp)
add_executable(art_simple_bench ${P_ART_BENCH})
target_link_libraries(art_simple_bench Indexes)

set(BENCHMARK perf/main.cpp)
add_executable(benchmark ${BENCHMARK})
target_link_libraries(benchmark Indexes)

add_executable(unittest
        ./test/test.cpp
        ./test/test_correctness.cpp
        ./test/test_nvm_mgr.cpp)
target_link_libraries(unittest Indexes gtest)

