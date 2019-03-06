// Copyright [2018] Alibaba Cloud All rights reserved

#include "engine_race.h"

#include <pthread.h>
#include <stdio.h>
#include <memory.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#include <fcntl.h>

#include "db_sharding.h"
#include "key_offset.h"
#include "indexer.h"
#include "util.h"
#include "atomic.h"

namespace polar_race {

//    static ATOMICLONG threadIdGen = 0;

    static uint32_t GetDBShardingIndex(uint64_t keyLong);

    static uint64_t GetKeyUint64(const PolarString &key);

    static bool BindCpuCore(uint32_t id) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(id, &mask);
        int ret = pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
        return ret == 0;
    }

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        printf("polardb merge file version 1205 17:46\n");
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

    EngineRace::EngineRace(const std::string &path)
            : mPath(path),
              mCacheManager(CacheBufferSize, DataFileSize, CacheItemSegmentCount) {
        mStart = GetCurrentTimeInUs();
    }

    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        RetCode ret = engine_race->init();

        if (ret == kSucc) {
            *eptr = engine_race;
        } else {
            delete engine_race;
        }
        return ret;
    }

    EngineRace::~EngineRace() {

        for (auto &mDBSharding : mDBShardings) {
            delete mDBSharding;
        }

        mDBShardings.clear();

        uint64_t cost = GetCurrentTimeInUs() - mStart;
        printf("total life cost:%lu us\n", cost);
    }

    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        uint64_t keyLong = GetKeyUint64(key);
        uint32_t shardingIndex = GetDBShardingIndex(keyLong);

        return mDBShardings[shardingIndex >> ShardingNumPreFileBits]->mDBShardingItems[shardingIndex &
                                                                                       ShardingNumPreFileHex]->Write(
                key, value,
                keyLong);
    }

    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        uint64_t keyLong = GetKeyUint64(key);
        uint32_t shardingIndex = GetDBShardingIndex(keyLong);

        return mDBShardings[shardingIndex >> ShardingNumPreFileBits]->mDBShardingItems[shardingIndex &
                                                                                       ShardingNumPreFileHex]->Read(key,
                                                                                                                    value,
                                                                                                                    keyLong);
    }

    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper, Visitor &visitor) {
        if (lower.size() == 0 && upper.size() == 0) {
            RetCode ret = kSucc;
            for (auto &mDBSharding : mDBShardings) {
                for (auto &shardingItem : mDBSharding->mDBShardingItems) {
                    ret = shardingItem->Range(visitor);
                    if (ret != kSucc) {
                        break;
                    }
                }
            }
            return ret;
        }

        return kNotSupported;
    }

    RetCode EngineRace::init() {
        PerfPoint initPoint("init polar db");

        if (access(mPath.c_str(), F_OK) != 0 &&
            mkdir(mPath.c_str(), 0777) != 0) {
            printf("mkdir error %d!\n", errno);
            return kIOError;
        }

        for (uint32_t i = 0; i < (1 << ShardingBits) / ShardingNumPreFile; ++i) {
            DBSharding *sharding = new DBSharding(mPath, i, &mCacheManager);
            mDBShardings.push_back(sharding);
        }

        Indexer *indexer = new Indexer(mDBShardings);

        RetCode rc = indexer->Load();

        if (rc != kSucc) {
            printf("load index file error %d!\n", errno);
            return rc;
        }

        return kSucc;
    }

    uint32_t GetDBShardingIndex(uint64_t keyLong) {
        return keyLong >> (64 - ShardingBits);
    }

    uint64_t GetKeyUint64(const PolarString &key) {
        uint64_t keyLong = 0;
        stringToUint64(key.data(), &keyLong);
        return keyLong;
    }

    void EngineRace::preRead(uint32_t threadId) {
        BindCpuCore(threadId);

        for (uint32_t i = 0; i < 2; ++i) {
            for (auto &mDBSharding : mDBShardings) {
                for (auto &shardingItem : mDBSharding->mDBShardingItems) {
                    shardingItem->PreReadSharding();
                }
            }
        }
    }

}  // namespace polar_race
