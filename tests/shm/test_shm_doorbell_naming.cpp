#include "plexus/shm/ring_layout.h"
#include "plexus/shm/doorbell_naming.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <array>
#include <string>
#include <cstdint>
#include <string_view>

using namespace plexus::shm;

namespace {

constexpr std::string_view k_region           = "0123456789abcdef";
constexpr std::array<std::uint32_t, 3> k_tags = {0, 1, 0xFFFF'FFFFu};

}

TEST_CASE("doorbell_naming every slot and tag yields a distinct name within the macOS semaphore budget", "[shm][doorbell_naming]")
{
    std::set<std::string> names;
    for(std::uint32_t slot = 0; slot < k_max_consumers; ++slot)
    {
        for(const std::uint32_t tag : k_tags)
        {
            const doorbell_name name = format_doorbell_name(k_semaphore_doorbell_prefix, k_region, slot, tag);
            REQUIRE(name.size == 29);
            REQUIRE(name.size <= k_doorbell_name_max);
            REQUIRE(std::string_view(name.c_str()) == name.view());
            REQUIRE(name.view().starts_with(k_semaphore_doorbell_prefix));
            names.insert(std::string(name.view()));
        }
    }
    REQUIRE(names.size() == k_max_consumers * k_tags.size());
}

TEST_CASE("doorbell_naming a name without a prefix is the bare region, slot and tag key", "[shm][doorbell_naming]")
{
    const doorbell_name name = format_doorbell_name({}, k_region, 10, 0x0000'2a2bu);

    REQUIRE(name.view() == "0123456789abcdefa00002a2b");
    REQUIRE(name.size == k_doorbell_key_chars);
}
