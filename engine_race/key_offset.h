// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_KEY_OFFSET_H_
#define ENGINE_RACE_KEY_OFFSET_H_

#include <assert.h>
#include <stdint.h>
#include <string>

#include "include/polar_string.h"
#include "util.h"

namespace polar_race {

    struct KeyOffset {
        uint64_t key;
        uint32_t offset;

        KeyOffset()
                : key(0),
                  offset(0) {
        }

        KeyOffset(uint64_t key, uint32_t offset)
                : key(key),
                  offset(offset) {
        }

    } __attribute__((packed));

    struct KeyAsc {
        bool operator()(const KeyOffset &left, const KeyOffset &right) const {
            if (left.key != right.key) {
                return left.key < right.key;
            }
            return left.offset < right.offset;
        }
    };

}  // polar_race

#endif  // ENGINE_RACE_KEY_OFFSET_H_
