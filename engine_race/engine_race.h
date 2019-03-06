// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <memory>
#include <string>
#include <vector>

#include "include/engine.h"
#include "cache_manager.h"
#include "key_offset.h"

namespace polar_race {

    class DBSharding;

    class EngineRace : public Engine {
    public:
        static RetCode Open(const std::string &path, Engine **eptr);

        explicit EngineRace(const std::string &path);

        ~EngineRace();

        RetCode Write(
                const PolarString &key,
                const PolarString &value) override;

        RetCode Read(
                const PolarString &key,
                std::string *value) override;

        RetCode Range(
                const PolarString &lower,
                const PolarString &upper,
                Visitor &visitor) override;

    private:
        RetCode init();

        void preRead(uint32_t threadId);

        std::string mPath;
        std::vector<DBSharding *> mDBShardings;

        CacheManager mCacheManager;
        uint64_t mStart;
    };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
