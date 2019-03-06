#ifndef ENGINE_CACHE_ITEM_H
#define ENGINE_CACHE_ITEM_H

#include <pthread.h>
#include <stdint.h>

#include "util.h"

namespace polar_race {

    class CacheItem {
    public:
        CacheItem();

        ~CacheItem();

        void WaitAllDataSegmentReady();

        uint32_t GetUnReadDataSegment() {
            uint32_t fillSegmentId;
            pthread_mutex_lock(&mMutex);

            if (mFilledSegment < mDataSegmentCount) {
                fillSegmentId = mFilledSegment++;
            } else {
                fillSegmentId = UINT32_MAX;
            }

            pthread_mutex_unlock(&mMutex);
            return fillSegmentId;

        }

        void SetDataSegmentReady();

        void ReleaseUsedRef();


        uint32_t mDBShardingIndex;
        void *mCacheDataPtr;
        uint32_t mUsedRef;
        uint32_t mDataSegmentCount;
        uint32_t mFilledSegment;
        uint32_t mCompletedSegment;

        pthread_mutex_t mMutex;
        pthread_cond_t mCondition;
    };

}  // namespace polar_race

#endif //ENGINE_CACHE_ITEM_H
