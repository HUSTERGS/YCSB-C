cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

project(ycsb)

include(FindThreads)
find_package(TBB REQUIRED)

add_subdirectory(redis)

#add_definitions(-DGENERATE_PREFIX_KEY)

link_directories(lib/wiredtiger/build)

add_executable(wiredtiger_test "")
target_sources(wiredtiger_test PUBLIC "WiredTigerTester.cpp")
target_include_directories(wiredtiger_test PUBLIC lib/wiredtiger/build/include)
target_link_libraries(wiredtiger_test PUBLIC wiredtiger)

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
        "core/histogram.cc"
        "core/histogram.h"

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
        "db/wiredtiger_db.cc"
        "db/wiredtiger_db.h"


        "lib/lock_stl_hashtable.h"
        "lib/mem_alloc.h"
        "lib/stl_hashtable.h"
        "lib/string.h"
        "lib/string_hashtable.h"
        "lib/tbb_scan_hashtable.h"
        "lib/tbb_rand_hashtable.h"

        "ycsbc.cc"
        )


# Generate wiredtiger.h include file.
#configure_file(lib/wiredtiger/src/include/wiredtiger.in lib/wiredtiger/src/include/wiredtiger.h @ONLY)
# Generate our wiredtiger_config.h include file.
#configure_file(lib/wiredtiger/cmake/configs/wiredtiger_config.h.in lib/wiredtiger/src/config/wiredtiger_config.h @ONLY)


## wiredtiger直接编译成shared
## 默认编译路径为build
## TODO，加一个script检查是否编译成功
target_include_directories(ycsb PUBLIC lib/wiredtiger/build/include)
target_link_libraries(ycsb PUBLIC wiredtiger)


target_include_directories(ycsb PUBLIC .)
target_link_libraries(ycsb PUBLIC ${CMAKE_THREAD_LIBS_INIT} TBB::tbb hiredis)