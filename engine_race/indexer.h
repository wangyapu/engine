// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_INDEXER_H_
#define ENGINE_RACE_INDEXER_H_

#include <pthread.h>
#include <string>
#include <vector>

#include "include/engine.h"
#include "key_offset.h"
#include "util.h"

namespace polar_race {

    class DBSharding;

    class Indexer {
    public:
        explicit Indexer(const std::vector<DBSharding *> &dbShardings);

        ~Indexer();

        RetCode Load();

    private:
        void loadDBSharding(uint32_t startIndex, uint32_t count, void *buffer);

        std::vector<DBSharding *> mDBShardings;
    };

}  // namespace polar_race

#endif  // ENGINE_RACE_INDEXER_H_
