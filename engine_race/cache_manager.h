#ifndef ENGINE_CACHE_MANAGER_H
#define ENGINE_CACHE_MANAGER_H

#include <stdint.h>

#include "cache_item.h"
#include "util.h"

namespace polar_race {

    class CacheManager {
    public:
        CacheManager(uint32_t mCacheItemCount,
                     uint32_t eachCacheItemSize,
                     uint32_t cacheItemSegmentCount);

        ~CacheManager();

        CacheItem *GetCacheItem(uint32_t dbShardingItemIndex);

        bool mStartPreRead;
        pthread_mutex_t mMutex;

    private:
        uint32_t mCacheItemCount;
        uint32_t mEachCacheItemSize;
        uint32_t mDataSegmentCount;

        CacheItem *mCacheItems;
    };


}  // namespace polar_race


#endif //ENGINE_CACHE_MANAGER_H
