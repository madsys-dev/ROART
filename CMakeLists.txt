cmake_minimum_required(VERSION 2.8)
project(P-ART)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

## Instruction options for Cache line flush
add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
#add_definitions(-DCLWB)
link_libraries(pthread atomic boost_system boost_thread gtest)
find_library(TbbLib tbb)
include_directories(./ART
                    ./benchmark
                    ./nvm_mgr
                    ./test)

set(INDEX_FILES 
        ./ART/Tree.cpp
        ./benchmark/generator.cpp
        ./nvm_mgr/nvm_mgr.cpp
        ./nvm_mgr/threadinfo.cpp)

add_library(Indexes ${INDEX_FILES})
target_link_libraries(Indexes  ${TbbLib})

set(P_ART_BENCH test/art_simple_bench.cpp)
add_executable(art_simple_bench ${P_ART_BENCH})
target_link_libraries(art_simple_bench Indexes)

set(P_ART_TEST test/art_correctness.cpp)
add_executable(art_test ${P_ART_TEST})
target_link_libraries(art_test Indexes)

set(BENCHMARK test/main.cpp)
add_executable(benchmark ${BENCHMARK})
target_link_libraries(benchmark Indexes)