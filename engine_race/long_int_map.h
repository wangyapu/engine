#ifndef ENGINE_LONG_INT_MAP_H
#define ENGINE_LONG_INT_MAP_H

#include <malloc.h>
#include <assert.h>
#include <cmath>
#include <chrono>

class LongIntMapForRace {
public:
    explicit LongIntMapForRace() {
        this->keyMixer = initialKeyMixer();
        this->assigned = 0;
        allocateBuffers();
    };

    ~LongIntMapForRace() {
        free(keys);
        free(values);
    };

    int size() {
        return assigned;
    }

    int put(const uint64_t &key, const uint32_t &value) {
        assert(this->assigned < mask + 1);
        if (key == 0) {
            hasEmptyKey = true;
            int previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            int slot = hashKey(key) & mask;
            uint64_t existing;
            while ((existing = keys[slot]) != 0) {
                if (existing == key) {
                    int previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }
            keys[slot] = key;
            values[slot] = value;
            assigned++;
            return 0;
        }

    };

    uint32_t getOrDefault(const uint64_t &key, const uint32_t &defaultValue) {
        if (key == 0) {
            return hasEmptyKey ? values[mask + 1] : defaultValue;
        } else {
            int slot = hashKey(key) & mask;
            uint64_t exsisting;
            while ((exsisting = keys[slot]) != 0) {
                if (exsisting == key) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }
            return defaultValue;
        }
    }

    uint64_t *keys;
    uint32_t *values;
    int keyMixer;
    int assigned;
    int mask;
    int hasEmptyKey;

    void allocateBuffers() {
        int arraySize = 128 * 1024;
        int emptyElementSlot = 1;
        this->keys = static_cast<uint64_t *>(malloc((arraySize + emptyElementSlot) * sizeof(uint64_t)));
        this->values = static_cast<uint32_t *>(malloc((arraySize + emptyElementSlot) * sizeof(uint32_t)));
        this->mask = arraySize - 1;

    }

    int hashKey(const uint64_t &key) {
        assert(key != 0);
        return mix(key, this->keyMixer);
    }

    int initialKeyMixer() {
        return static_cast<int>(-1686789945);
    }

    int mix(uint64_t key, int seed) {
        return static_cast<int>(mix64(key ^ seed));
    }

    uint64_t mix64(uint64_t z) {
        z = (z ^ (z >> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >> 32);
    }

};

#endif //ENGINE_LONG_INT_MAP_H
