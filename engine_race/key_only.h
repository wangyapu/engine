// Copyright [2018] Alibaba Cloud All rights reserved

#ifndef ENGINE_RACE_KEY_ONLY_H_
#define ENGINE_RACE_KEY_ONLY_H_

#include <string>

namespace polar_race {

    struct KeyOnly {
        uint64_t key;

        KeyOnly()
                : key(0) {
        }

    } __attribute__((packed));

}  // polar_race

#endif  // ENGINE_RACE_KEY_ONLY_H_
