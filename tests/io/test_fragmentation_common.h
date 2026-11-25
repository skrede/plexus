#ifndef HPP_GUARD_TESTS_IO_TEST_FRAGMENTATION_COMMON_H
#define HPP_GUARD_TESTS_IO_TEST_FRAGMENTATION_COMMON_H

#include "plexus/datagram/detail/reassembler.h"

#include "plexus/io/fragmentation.h"

#include "plexus/datagram/mtu_budget.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/testing/harness.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

using namespace plexus;
using namespace std::chrono_literals;

namespace fragmentation_fixture {

inline std::vector<std::byte> bytes_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> out;
    out.reserve(vals.size());
    for(int v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

#endif
