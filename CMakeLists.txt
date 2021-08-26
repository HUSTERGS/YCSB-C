cmake_minimum_required(VERSION 3.14)
project(ycsb)

include(FindThreads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(TBB REQUIRED tbb)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

add_subdirectory(redis)

option(WITH_PMEM_ROCKSDB "support the pmem rocksdb" OFF)
option(WITH_CCEH "support for CCEH hash table" ON)

add_executable(ycsb "")
target_sources(ycsb PUBLIC
        "core/client.h"
        "core/const_generator.h"
        "core/core_workload.cc"
        "core/core_workload.h"
        "core/counter_generator.h"
        "core/db.h"
        "core/discrete_generator.h"
        "core/generator.h"
        "core/properties.h"
        "core/scrambled_zipfian_generator.h"
        "core/skewed_latest_generator.h"
        "core/timer.h"
        "core/uniform_generator.h"
        "core/utils.h"
        "core/zipfian_generator.h"

        "db/basic_db.h"
        "db/db_factory.cc"
        "db/db_factory.h"
        "db/hashtable_db.cc"
        "db/hashtable_db.h"
        "db/lock_stl_db.h"
        "db/redis_db.cc"
        "db/redis_db.h"
        "db/tbb_rand_db.h"
        "db/tbb_scan_db.h"
        #"db/log_db.cc"
        #"db/log_db.h"

        "db/cceh_db.cc"
        "db/cceh_db.h"

        "lib/lock_stl_hashtable.h"
        "lib/mem_alloc.h"
        "lib/stl_hashtable.h"
        "lib/string.h"
        "lib/string_hashtable.h"
        "lib/tbb_scan_hashtable.h"
        "lib/tbb_rand_hashtable.h"
        #"lib/pm_log_store.h"

        "ycsbc.cc"
        )



if (WITH_PMEM_ROCKSDB)
    add_definitions(-DWITH_DCPMM)
    add_definitions(-DON_DCPMM)
    add_definitions(-DUSING_PMEM_ROCKSDB)
    add_subdirectory(lib/pmem-rocksdb)
    target_include_directories(ycsb PUBLIC lib/pmem-rocksdb/include)
    target_sources(ycsb PUBLIC
            "db/pmem_rocksdb_db.cc"
            "db/pmem_rocksdb_db.h"
            )
    target_link_libraries(ycsb rocksdb)
endif ()

if (WITH_CCEH)
    add_definitions(-DUSING_CCEH)
    add_subdirectory(lib/cceh)
    target_include_directories(ycsb PUBLIC lib/cceh/CCEH-PMDK/src ${LIBPMEMOBJ_INCLUDE_DIRS})
    target_sources(ycsb PUBLIC
            "db/cceh_db.cc"
            "db/cceh_db.h"
            )
    target_link_libraries(ycsb cceh ${LIBPMEMOBJ_LIBRARIES})
endif ()

target_include_directories(ycsb PUBLIC .)
target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${TBB_LIBRARIES} ${LIBPMEM_LIBRARIES} hiredis)