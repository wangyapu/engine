cmake_minimum_required(VERSION 3.12)
project(engine)

set(CMAKE_CXX_STANDARD 14)

include_directories(engine_race)
include_directories(include)

add_executable(engine
        engine_race/atomic.h
        engine_race/db_sharding.cc
        engine_race/db_sharding.h
        engine_race/db_sharding_item.h
        engine_race/db_sharding_item.cc
        engine_race/engine_race.cc
        engine_race/engine_race.h
        engine_race/key_offset.h
        engine_race/key_only.h
        engine_race/indexer.cc
        engine_race/indexer.h
        engine_race/util.cc
        engine_race/util.h
        include/engine.h
        include/polar_string.h
        test/test.cc engine_race/global.h
        engine_race/cache_manager.h
        engine_race/cache_manager.cc
        engine_race/cache_item.h
        engine_race/cache_item.cc engine_race/long_int_map.h)
