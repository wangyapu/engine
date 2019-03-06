#include "db_sharding.h"
#include "key_only.h"

#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <include/engine.h>
#include <thread>

namespace polar_race {
    static const char *DataFilePrefix = "DATA_";
    static const char *FileSplit = "/";

    DBSharding::DBSharding(const std::string &path, uint32_t index, CacheManager *cacheManager)
            : mPath(path),
              mShardingIndex(index),
              mDataPath(mPath + FileSplit + DataFilePrefix + std::to_string(mShardingIndex)),
              mDataDirectFd(-1),
              mCacheManager(cacheManager) {

        for (uint32_t i = 0; i < ShardingNumPreFile; ++i) {
            DBShardingItem *shardingItem = new DBShardingItem(path, mShardingIndex * ShardingNumPreFile + i,
                                                              mDataDirectFd, cacheManager);
            mDBShardingItems.push_back(shardingItem);
        }

    }

    DBSharding::~DBSharding() {
        for (auto &mDBShardingItem : mDBShardingItems) {
            delete mDBShardingItem;
        }

        mDBShardingItems.clear();

        if (mDataDirectFd >= 0) {
            close(mDataDirectFd);
            mDataDirectFd = -1;
        }

    }

    RetCode DBSharding::LoadSharding(void *buffer) {
        InitDBShardingFile();

        return LoadAllIndex(buffer);
    }

    RetCode DBSharding::preFallocate() {
//        mTotalKey = 0;

        mDataDirectFd = open(mDataPath.c_str(), O_DIRECT | O_NOATIME | O_RDWR | O_CREAT, 0644);
        if (mDataDirectFd < 0) {
            return kIOError;
        }
        if (posix_fallocate(mDataDirectFd, 0, DataFileSize * ShardingNumPreFile) != 0) {
            printf("Failed to fallocate! %s\n", mDataPath.c_str());
            return kIOError;
        }

        return kSucc;
    }

    RetCode DBSharding::InitDBShardingFile() {
        preFallocate();
//        mDataDirectFd = open(mDataPath.c_str(), O_DIRECT | O_NOATIME | O_RDWR, 0644);
//
//        if (mDataDirectFd < 0) {
//            return preFallocate();
//        }
        return kSucc;
    }

    RetCode DBSharding::LoadAllIndex(void *buffer) {
        for (auto &mDBShardingItem : mDBShardingItems) {
            mDBShardingItem->mDataDirectFd = mDataDirectFd;
            mDBShardingItem->LoadAllIndex(buffer);
        }
        return kSucc;
    }

}  // namespace polar_race
