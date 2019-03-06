#include "db_sharding_item.h"
#include "key_only.h"

#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <include/engine.h>
#include <thread>

namespace polar_race {
    static const char *SegmentFilePrefix = "SEGMENT_";
    static const char *IndexFilePrefix = "INDEX_";
    static const char *FileSplit = "/";

    static const uint32_t IndexFileSize = sizeof(KeyOnly) << 15;
    static const uint32_t MergeLimit = 4;
    static const uint32_t MergeBufferSize = MergeLimit * 4096;
    static uint32_t EachSegmentSize = DataFileSize / CacheItemSegmentCount;

    DBShardingItem::DBShardingItem(const std::string &path, uint32_t itemIndex, int dataDirectFd,
                                   CacheManager *cacheManager)
            : mDataDirectFd(dataDirectFd),
              mShardingItemIndex(itemIndex),
              mTotalKey(0),
              mSegmentPath(path + FileSplit + SegmentFilePrefix + std::to_string(itemIndex)),
              mIndexPath(path + FileSplit + IndexFilePrefix + std::to_string(itemIndex)),
              mSegmentDataFd(-1),
              mIndexFd(-1),
              mIndexPtr(NULL),
              mSegmentBuffer(NULL),
              mStartPosition(itemIndex % ShardingNumPreFile * DataFileSize),
              mWritePosition(mStartPosition),
              mSegmentBufferIndex(0),
              mCacheManager(cacheManager) {

        if (mKeyOffsets.empty()) {
            mKeyOffsets.reserve(1 << 15);
        }

        mIndexFd = open(mIndexPath.c_str(), O_NOATIME | O_RDWR, 0644);

        if (mIndexFd < 0) {
            preFallocate();
        } else {
            mIndexPtr = mmap(NULL, IndexFileSize, PROT_READ | PROT_WRITE,
                             MAP_SHARED, mIndexFd, 0);

            mSegmentDataFd = open(mSegmentPath.c_str(), O_NOATIME | O_RDWR, 0644);

            mSegmentBuffer = mmap(NULL, MergeBufferSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
                                  mSegmentDataFd, 0);
        }

        pthread_mutex_init(&mMutex, NULL);
        pthread_cond_init(&mCondition, NULL);
    }

    DBShardingItem::~DBShardingItem() {
        if (mSegmentBuffer != NULL) {
            if (mSegmentBufferIndex != 0) {
                uint32_t remainBufferSize = mSegmentBufferIndex * 4096;
                pwrite64(mDataDirectFd, mSegmentBuffer, remainBufferSize, mWritePosition);
            }

            munmap(mSegmentBuffer, MergeBufferSize);
            mSegmentBuffer = NULL;
        }

        if (mSegmentDataFd >= 0) {
            close(mSegmentDataFd);
            mSegmentDataFd = -1;
        }
        if (mIndexPtr != NULL) {
            munmap(mIndexPtr, IndexFileSize);
            mIndexPtr = NULL;
        }
        if (mIndexFd >= 0) {
            close(mIndexFd);
            mIndexFd = -1;
        }

        pthread_mutex_destroy(&mMutex);
        pthread_cond_destroy(&mCondition);
    }

    RetCode DBShardingItem::Write(const PolarString &key, const PolarString &value, uint64_t keyLong) {
        KeyOnly keyOnly;
        keyOnly.key = keyLong;
        KeyOnly *ptr = reinterpret_cast<KeyOnly *>(mIndexPtr);

        pthread_mutex_lock(&mMutex);

//        memcpy_4k(static_cast<char *>(mSegmentBuffer) + mSegmentBufferIndex * 4096, value.data());

        memcpy(static_cast<char *>(mSegmentBuffer) + mSegmentBufferIndex * 4096, value.data(), 4096);
        mSegmentBufferIndex++;

        if (mSegmentBufferIndex == MergeLimit) {
            pwrite64(mDataDirectFd, mSegmentBuffer, MergeBufferSize, mWritePosition);

            mWritePosition += MergeBufferSize;
            mSegmentBufferIndex = 0;
        }

        ptr[mTotalKey] = keyOnly;

        mTotalKey++;

        pthread_mutex_unlock(&mMutex);

        return kSucc;
    }

    RetCode DBShardingItem::Read(const PolarString &key, std::string *value, uint64_t keyLong) {
        uint32_t offset = binarySearch(keyLong);

        if (unlikely(offset == UINT32_MAX)) {
            return kNotFound;
        }

        static __thread void *readBuffer = NULL;
        if (unlikely(readBuffer == NULL)) {
            posix_memalign(&readBuffer, getpagesize(), 4096);
        }

        if (unlikely(value->size() != 4096)) {
            value->resize(4096);
        }

        RetCode ret = readValue(offset - 1, readBuffer);
        memcpy(&((*value)[0]), readBuffer, 4096);
//        memcpy_4k(&((*value)[0]), readBuffer);
        return ret;
    }

    RetCode DBShardingItem::Range(Visitor &visitor) {
        char key[8];
        for (auto mKeyOffset = mKeyOffsets.begin(); mKeyOffset != mKeyOffsets.end(); mKeyOffset++) {
            if (unlikely(mKeyOffset->key == (mKeyOffset + 1)->key)) {
                continue;
            }
            char ptr[4096];
            uint64ToString(mKeyOffset->key, key);
            PolarString str(key, 8);
            readValueForRange(mKeyOffset->offset - 1, ptr);
            PolarString value(ptr, 4096);
            visitor.Visit(str, value);
        }
        return kSucc;
    }

    RetCode DBShardingItem::readValue(uint32_t offset, void *value) {
        uint64_t dataOffset = (offset << 12) + mStartPosition;

        pread64(mDataDirectFd, value, 4096, dataOffset);

        return kSucc;
    }

    RetCode DBShardingItem::readValueForRange(uint32_t offset, void *value) {
        uint64_t dataOffset = (offset << 12) + mStartPosition;

        static __thread void *readBuffer = NULL;
        if (unlikely(readBuffer == NULL)) {
            posix_memalign(&readBuffer, getpagesize(), 4096);
        }

        pread64(mDataDirectFd, readBuffer, 4096, dataOffset);

        memcpy(value, readBuffer, 4096);

        return kSucc;
    }

    RetCode DBShardingItem::LoadAllIndex(void *buffer) {
        int batchSize = sizeof(KeyOnly) * BatchLoadKeyCount;
        uint64_t offset = 0;
        bool finish = false;

        while (!finish) {
            if (unlikely(pread64(mIndexFd, buffer, batchSize, offset) < batchSize)) {
                return kIOError;
            }
            KeyOnly *ptr = reinterpret_cast<KeyOnly *>(buffer);
            for (int i = 0; i < BatchLoadKeyCount; ++i) {
                uint64_t key = ptr[i].key;
                if (key == 0) {
                    finish = true;
                    break;
                }

                mKeyOffsets.emplace_back(key, mTotalKey + 1);
                mTotalKey++;
            }
            offset += batchSize;
        }

        mWritePosition = mStartPosition + mTotalKey * 4096;

        if (!mKeyOffsets.empty()) {
            std::sort(mKeyOffsets.begin(), mKeyOffsets.end(), KeyAsc());

            posix_fadvise(mIndexFd, 0, IndexFileSize, POSIX_FADV_DONTNEED);
        }
        return kSucc;
    }

    void DBShardingItem::PreReadSharding() {
        if (mTotalKey == 0) {
            return;
        }

        CacheItem *item = NULL;
        while (true) {
            item = mCacheManager->GetCacheItem(mShardingItemIndex);
            if (item != NULL) {
                break;
            }
        }

        while (true) {
            uint32_t segmentNo = item->GetUnReadDataSegment();
            if (segmentNo == UINT32_MAX) {
                break;
            }

            uint64_t cacheOffset = segmentNo * EachSegmentSize;
            uint64_t dataOffset = cacheOffset + mStartPosition;
            pread64(mDataDirectFd, static_cast<char *>(item->mCacheDataPtr) + cacheOffset, EachSegmentSize, dataOffset);

            item->SetDataSegmentReady();
        }

        item->ReleaseUsedRef();
    }

    RetCode DBShardingItem::RangeScan(Visitor &visitor) {
        if (unlikely(mTotalKey == 0)) {
            return kSucc;
        }
        CacheItem *item = NULL;

        while (true) {
            item = mCacheManager->GetCacheItem(mShardingItemIndex);
            if (item != NULL) {
                break;
            }
            usleep(1);
        }
        item->WaitAllDataSegmentReady();

        char key[8];
        for (auto mKeyOffset = mKeyOffsets.begin(); mKeyOffset != mKeyOffsets.end(); mKeyOffset++) {
            if (unlikely(mKeyOffset->key == (mKeyOffset + 1)->key)) {
                continue;
            }

            uint32_t offset = mKeyOffset->offset - 1;

            char *ptr = static_cast<char *>(item->mCacheDataPtr) + offset * 4096;

            uint64ToString(mKeyOffset->key, key);
            PolarString str(key, 8);
            PolarString value(ptr, 4096);
            visitor.Visit(str, value);
        }
        item->ReleaseUsedRef();
        item = NULL;
        return kSucc;
    }

    static bool keyOffsetCompareLong(const uint64_t &key, const KeyOffset &keyOffset) {
        return key < keyOffset.key;
    }

    uint32_t DBShardingItem::binarySearch(uint64_t key) {
        auto found = std::upper_bound(this->mKeyOffsets.begin(), this->mKeyOffsets.end(), key, keyOffsetCompareLong);
        if (unlikely(found == this->mKeyOffsets.begin() || (--found)->key != key)) {
            return UINT32_MAX;
        } else {
            return found->offset;
        }

    }

    RetCode DBShardingItem::preFallocate() {
        mIndexFd = open(mIndexPath.c_str(), O_NOATIME | O_RDWR | O_CREAT, 0644);

        posix_fallocate(mIndexFd, 0, IndexFileSize);
        mIndexPtr = mmap(NULL, IndexFileSize, PROT_READ | PROT_WRITE,
                         MAP_SHARED, mIndexFd, 0);

        mSegmentDataFd = open(mSegmentPath.c_str(), O_CREAT | O_NOATIME | O_RDWR, 0644);

        posix_fallocate(mSegmentDataFd, 0, MergeBufferSize);

        mSegmentBuffer = mmap(NULL, MergeBufferSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
                              mSegmentDataFd, 0);
        return kSucc;
    }

}  // namespace polar_race
