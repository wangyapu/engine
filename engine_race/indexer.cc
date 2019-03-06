#include "indexer.h"

#include <thread>

#include <unistd.h>

#include "db_sharding.h"
#include "key_only.h"

namespace polar_race {

    static const uint32_t kParallelThreadBits = 6; // 64 thread

    Indexer::Indexer(const std::vector<DBSharding *> &dbShardings)
            : mDBShardings(dbShardings) {
    }

    Indexer::~Indexer() {
    }

    RetCode Indexer::Load() {
        PerfPoint initPoint("Load DB Shardings");
        uint32_t threadCount = 1 << kParallelThreadBits;
        std::vector<std::thread> threads(threadCount);
        uint32_t count = (1 << (ShardingBits - kParallelThreadBits)) / ShardingNumPreFile;

        for (uint32_t i = 0; i < threadCount; ++i) {
            void *buffer = NULL;
            int batchSize = sizeof(KeyOnly) * BatchLoadKeyCount;
            posix_memalign(&buffer, getpagesize(), batchSize);

            threads[i] = std::thread(&Indexer::loadDBSharding, this, count * i, count, buffer);
        }

        for (uint32_t i = 0; i < threadCount; ++i) {
            threads[i].join();
        }

        return kSucc;
    }

    void Indexer::loadDBSharding(uint32_t startIndex, uint32_t count, void *buffer) {
        for (uint32_t i = startIndex; i < startIndex + count; ++i) {
            mDBShardings[i]->LoadSharding(buffer);
        }

        free(buffer);
        buffer = NULL;
    }

}  // namespace polar_race
