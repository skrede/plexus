#include "plexus/datagram/detail/seq_ring.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using ring = plexus::datagram::detail::seq_ring<std::int32_t>;

TEST_CASE("seq_ring: a window that does not divide the sequence space never maps two held sequences to one slot", "[io][seq_ring]")
{
    for(const std::size_t window : std::array<std::size_t, 3>{3, 5, 500})
    {
        for(std::uint32_t back = 0; back <= window + 1; ++back)
        {
            const auto base = static_cast<std::uint16_t>(65536 - back);
            ring r{window, static_cast<std::uint16_t>(base - 7)};
            for(std::uint32_t i = 0; i < 7; ++i)
                r.advance();
            REQUIRE(r.base() == base);

            std::set<const std::int32_t *> slots;
            for(std::uint32_t d = 0; d < window; ++d)
            {
                const auto seq = static_cast<std::uint16_t>(base + d);
                REQUIRE(r.holds(seq));
                slots.insert(&r.at(seq));
            }
            REQUIRE(slots.size() == window);
            REQUIRE_FALSE(r.holds(static_cast<std::uint16_t>(base + window)));
            REQUIRE_FALSE(r.holds(static_cast<std::uint16_t>(base - 1)));
        }
    }
}

TEST_CASE("seq_ring: advance moves the base sequence and the base index together", "[io][seq_ring]")
{
    for(const std::size_t window : std::array<std::size_t, 2>{3, 5})
    {
        const std::uint16_t start = 65533;
        ring r{window, start};
        REQUIRE(r.window() == window);

        std::vector<const std::int32_t *> before;
        for(std::uint32_t k = 0; k < window; ++k)
            before.push_back(&r.at(static_cast<std::uint16_t>(start + k)));

        for(std::uint32_t k = 1; k <= 12; ++k)
        {
            r.advance();
            REQUIRE(r.base() == static_cast<std::uint16_t>(start + k));
            REQUIRE(&r.at(r.base()) == before[k % window]);
        }
    }
}
