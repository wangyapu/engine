#ifndef ENGINE_RACE_DB_SHARDING_ITEM_H_
#define ENGINE_RACE_DB_SHARDING_ITEM_H_

#include <pthread.h>
#include <string>
#include <vector>

#include "include/engine.h"
#include "db_sharding.h"
#include "key_offset.h"
#include "util.h"
#include "global.h"
#include "cache_manager.h"
#include "long_int_map.h"

namespace polar_race {
    class DBShardingItem {
    public:
        DBShardingItem(const std::string &path, uint32_t itemIndex, int dataDirectFd, CacheManager *cacheManager);

        ~DBShardingItem();

        RetCode Write(const PolarString &key, const PolarString &value, uint64_t keyLong);

        RetCode Read(const PolarString &key, std::string *value, uint64_t keyLong);

        RetCode Range(Visitor &visitor);

        RetCode LoadAllIndex(void *buffer);

        void PreReadSharding();

        RetCode RangeScan(Visitor &visitor);

        RetCode preFallocate();

        int mDataDirectFd;

        std::vector<KeyOffset> mKeyOffsets;

    private:
        RetCode readValue(uint32_t offset, void *value);

        RetCode readValueForRange(uint32_t offset, void *value);

        uint32_t binarySearch(uint64_t keyIndex);

        uint32_t mShardingItemIndex;
        uint32_t mTotalKey;

        std::string mSegmentPath;
        std::string mIndexPath;
        int mSegmentDataFd;
        int mIndexFd;
        void *mIndexPtr;

        void *mSegmentBuffer;

        uint64_t mStartPosition;
        uint64_t mWritePosition;
        uint32_t mSegmentBufferIndex;

        CacheManager *mCacheManager;

        pthread_mutex_t mMutex;
        pthread_cond_t mCondition;

    };

}  // namespace polar_race

#endif  // ENGINE_RACE_DB_SHARDING_ITEM_H_
