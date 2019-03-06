#include "cache_item.h"

#include "cache_manager.h"
#include "global.h"

#include <unistd.h>

namespace polar_race {

    CacheItem::CacheItem()
            : mDBShardingIndex(UINT32_MAX),
              mCacheDataPtr(NULL),
              mUsedRef(0),
              mDataSegmentCount(0),
              mCompletedSegment(0) {
        posix_memalign(&mCacheDataPtr, DataFileSize / CacheItemSegmentCount, DataFileSize);

        pthread_mutex_init(&mMutex, NULL);
        pthread_cond_init(&mCondition, NULL);
    }

    CacheItem::~CacheItem() {
        pthread_mutex_destroy(&mMutex);
        pthread_cond_destroy(&mCondition);

        if (mCacheDataPtr != NULL) {
            free(mCacheDataPtr);
            mCacheDataPtr = NULL;
        }
    }

    void CacheItem::WaitAllDataSegmentReady() {
        pthread_mutex_lock(&mMutex);

        while (mCompletedSegment < mDataSegmentCount) {
            pthread_cond_wait(&mCondition, &mMutex);
        }

        pthread_mutex_unlock(&mMutex);
    }

    void CacheItem::SetDataSegmentReady() {
        pthread_mutex_lock(&mMutex);
        mCompletedSegment++;

        if (mCompletedSegment >= mDataSegmentCount) {
            pthread_cond_broadcast(&mCondition);
        }

        pthread_mutex_unlock(&mMutex);
    }

    void CacheItem::ReleaseUsedRef() {
        pthread_mutex_lock(&mMutex);
        mUsedRef--;
        pthread_mutex_unlock(&mMutex);
    }

}  // namespace polar_race
