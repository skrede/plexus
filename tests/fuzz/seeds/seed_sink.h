#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_SEEDS_SEED_SINK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_SEEDS_SEED_SINK_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <initializer_list>

using seed     = std::vector<std::byte>;
using seed_set = std::vector<seed>;

inline void append(seed &out, std::span<const std::byte> bytes)
{
    out.insert(out.end(), bytes.begin(), bytes.end());
}

inline void append(seed &out, std::initializer_list<std::uint8_t> bytes)
{
    for(const std::uint8_t b : bytes)
        out.push_back(std::byte{b});
}

seed_set crc_serial_seeds();

#endif
