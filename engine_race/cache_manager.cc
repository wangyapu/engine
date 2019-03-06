#include "cache_manager.h"

#include <unistd.h>

namespace polar_race {

    CacheManager::CacheManager(uint32_t mCacheItemCount,
                               uint32_t mEachCacheItemSize,
                               uint32_t mDataSegmentCount)
            : mStartPreRead(false),
              mCacheItemCount(mCacheItemCount),
              mEachCacheItemSize(mEachCacheItemSize),
              mDataSegmentCount(mDataSegmentCount) {
        mCacheItems = new CacheItem[mCacheItemCount];
        pthread_mutex_init(&mMutex, NULL);
    }

    CacheManager::~CacheManager() {
        delete[] mCacheItems;
        mCacheItems = NULL;

        mStartPreRead = false;
        pthread_mutex_destroy(&mMutex);
    }

    CacheItem *CacheManager::GetCacheItem(uint32_t dbShardingItemIndex) {
        uint32_t index = dbShardingItemIndex % mCacheItemCount;
        auto cacheItem = mCacheItems + index;

        pthread_mutex_lock(&(cacheItem->mMutex));
        if (cacheItem->mDBShardingIndex == dbShardingItemIndex) {

            cacheItem->mUsedRef++;

            pthread_mutex_unlock(&(cacheItem->mMutex));
            return cacheItem;
        }

        if (cacheItem->mUsedRef > 0) {
            pthread_mutex_unlock(&(cacheItem->mMutex));
            return NULL;
        }

        cacheItem->mDBShardingIndex = dbShardingItemIndex;
        cacheItem->mDataSegmentCount = mDataSegmentCount;
        cacheItem->mFilledSegment = 0;
        cacheItem->mCompletedSegment = 0;
        cacheItem->mUsedRef = 1;

        pthread_mutex_unlock(&(cacheItem->mMutex));

        return cacheItem;
    }

}  // namespace polar_race
