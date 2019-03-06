#ifndef ENGINE_GLOBAL_H
#define ENGINE_GLOBAL_H

#include <string>
#include "key_offset.h"

//#define USE_MAP_FOR_READ

namespace polar_race {
    // open
    static const int BatchLoadKeyCount = 4096;

    static const uint32_t ShardingBits = 11;
    static const uint64_t DataFileSize = 1 << 27;
    static const uint32_t CacheBufferSize = 8;
    static const uint32_t CacheItemSegmentCount = 2;

    // range
    static const uint32_t BatchPreReadKeyCount = 1024 * 32;
    static const uint32_t PreReadThreadNum = 2;

    // merge file
    static const uint32_t ShardingNumPreFile = 1 << 5;
    static const uint32_t ShardingNumPreFileBits = 5;
    static const uint32_t ShardingNumPreFileHex = 0x1f;


}  // namespace polar_race

#endif //ENGINE_GLOBAL_H
