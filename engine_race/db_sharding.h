// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_DB_SHARDING_H_
#define ENGINE_RACE_DB_SHARDING_H_

#include <pthread.h>
#include <string>
#include <vector>

#include "include/engine.h"
#include "db_sharding_item.h"
#include "key_offset.h"
#include "util.h"
#include "global.h"
#include "cache_manager.h"
#include "long_int_map.h"

namespace polar_race {
    class DBShardingItem;

    class DBSharding {
    public:
        DBSharding(const std::string &path, uint32_t index, CacheManager *cacheManager);

        ~DBSharding();

        RetCode LoadSharding(void *buffer);

        RetCode InitDBShardingFile();

        RetCode LoadAllIndex(void *buffer);

        std::vector<DBShardingItem *> mDBShardingItems;

    private:
        RetCode preFallocate();

        std::string mPath;
        uint32_t mShardingIndex;
        std::string mDataPath;

        int mDataDirectFd;

        CacheManager *mCacheManager;

    };

}  // namespace polar_race

#endif  // ENGINE_RACE_DB_SHARDING_H_
