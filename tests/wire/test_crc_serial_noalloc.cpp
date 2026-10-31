#include "test_crc_serial_common.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace crc_serial_test;

namespace {

constexpr std::size_t k_cap       = 256;
constexpr std::size_t k_max_frame = serial_header_size + k_cap + crc_trailer_size;

std::vector<std::byte> full_frames(std::size_t count)
{
    const std::string payload(k_cap, 'x');
    std::vector<std::byte> out;
    for(std::size_t i = 0; i < count; ++i)
    {
        const auto f = framed(payload);
        out.insert(out.end(), f.begin(), f.end());
    }
    return out;
}

std::vector<std::byte> anchored_garbage(std::size_t size)
{
    std::vector<std::byte> out(size, std::byte{0xA5});
    for(std::size_t i = 0; i + 1 < size; i += 3)
    {
        out[i]     = k_serial_format_id[0];
        out[i + 1] = k_serial_format_id[1];
    }
    return out;
}

template<typename F>
std::size_t allocations_during(F &&step)
{
    const auto before = plexus::testing::alloc_count();
    step();
    return plexus::testing::alloc_count() - before;
}

}

TEST_CASE("crc_serial noalloc: the deframer allocates nothing after construction, whatever it is fed", "[wire][crc_serial][noalloc]")
{
    std::int32_t emits = 0;
    std::int32_t drops = 0;
    crc_serial_inbound dec{k_cap};
    dec.on_match([&](std::span<const std::byte>) { ++emits; });
    dec.on_drop([&](close_cause) { ++drops; });

    const auto valid   = full_frames(3);
    const auto garbage = anchored_garbage(2 * k_max_frame);
    auto damaged       = full_frames(1);
    damaged[3] ^= std::byte{0x01};
    damaged.insert(damaged.end(), valid.begin(), valid.end());
    REQUIRE(valid.size() == 3 * k_max_frame);

    dec.feed(framed("warm"));
    REQUIRE(emits == 1);

    REQUIRE(allocations_during([&] { dec.feed(valid); }) == 0);
    REQUIRE(emits == 4);

    const std::span<const std::byte> bytes{valid};
    REQUIRE(allocations_during(
                    [&]
                    {
                        for(std::size_t i = 0; i < bytes.size(); ++i)
                            dec.feed(bytes.subspan(i, 1));
                    }) == 0);
    REQUIRE(emits == 7);

    REQUIRE(allocations_during([&] { dec.feed(garbage); }) == 0);
    REQUIRE(emits == 7);
    REQUIRE(drops > 0);

    REQUIRE(allocations_during([&] { dec.feed(damaged); }) == 0);
    REQUIRE(emits == 10);
}
