cmake_minimum_required(VERSION 2.8)
project(P-ART)

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE Release)
else ()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O0 -g -march=native -mavx -mavx2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O0 -g -march=native -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -mavx -mavx2")

## Instruction options for Cache line flush
#add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
add_definitions(-DCLWB)
add_definitions(-DVARIABLE_LENGTH)
add_definitions(-DUSE_PMDK)
add_definitions(-DACMA) # for fastfair and skiplist with dcmm
add_definitions(-DFF_GC) # ff_gc
add_definitions(-DRECLAIM_MEMORY)
add_definitions(-DKEY_INLINE)
#add_definitions(-DARTPMDK) # for DLART with PMDK
#add_definitions(-DCOUNT_ALLOC)
#add_definitions(-DLOG_FREE)
#add_definitions(-DCHECK_COUNT)
add_definitions(-DINSTANT_RESTART)

add_definitions(-DLEAF_ARRAY)

#malloc
#add_definitions(-DPMALLOC)
#add_definitions(-DTXPMALLOC)
add_definitions(-DTRANSACTIONAL)

link_libraries(pthread atomic boost_system boost_thread gtest)
link_libraries(pmemobj)
find_library(TbbLib tbb)

include_directories(./ART
        ./benchmark
        ./nvm_mgr
        ./test
        ./fast_fair
        ./lf-skiplist)

aux_source_directory(ART DIR_ART_SRCS)
aux_source_directory(benchmark DIR_BENCH_SRCS)
aux_source_directory(fast_fair DIR_FF_SRCS)
aux_source_directory(lf-skiplist DIR_SL_SRCS)
aux_source_directory(nvm_mgr   DIR_NVMMGR_SRCS)
aux_source_directory(perf      DIR_PERF_SRCS)
aux_source_directory(test      DIR_TEST_SRC)

add_library(Indexes ${DIR_ART_SRCS} ${DIR_BENCH_SRCS} ${DIR_FF_SRCS} ${DIR_NVMMGR_SRCS} ${DIR_SL_SRCS} )
target_link_libraries(Indexes ${TbbLib})


#set(P_ART_BENCH perf/art_simple_bench.cpp)
#add_executable(art_simple_bench ${P_ART_BENCH})
#target_link_libraries(art_simple_bench Indexes)

set(BENCHMARK perf/main.cpp)
add_executable(benchmark ${BENCHMARK})
target_link_libraries(benchmark Indexes)

set(MALLOC_DIFF perf/malloc_diff.cpp)
add_executable(malloc_diff ${MALLOC_DIFF})
target_link_libraries(malloc_diff Indexes)

add_executable(unittest ${DIR_TEST_SRC})
target_link_libraries(unittest Indexes gtest)

#add_executable(gen_mail script/gen_mail.cpp)
#add_executable(extract_data script/extract_data.cpp)

add_executable(acmma perf/acmma.cpp)
target_link_libraries(acmma Indexes)