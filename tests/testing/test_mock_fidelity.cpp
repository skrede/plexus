#include "plexus/testing/harness.h"
#include "plexus/testing/mock_policy.h"
#include "plexus/testing/test_clock.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>

using plexus::testing::harness;
using plexus::testing::mock_channel;
using plexus::testing::test_clock;

// Compile-time gate: the recording mock satisfies the same concept the production
// channels do, so code templated on a byte_channel accepts it unchanged.
static_assert(plexus::io::byte_channel<mock_channel<test_clock>>);

namespace {

std::vector<std::byte> bytes_of(std::initializer_list<unsigned char> vs)
{
    std::vector<std::byte> out;
    for(auto v : vs)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

TEST_CASE("mock byte_channel records sent bytes verbatim and in order", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    const auto first  = bytes_of({0x01, 0x02, 0x03});
    const auto second = bytes_of({0xAA, 0xBB});

    ch.send(std::span<const std::byte>(first));
    ch.send(std::span<const std::byte>(second));

    REQUIRE(ch.sent_packets() == 2);
    REQUIRE(ch.sent()[0] == first);
    REQUIRE(ch.sent()[1] == second);
}

TEST_CASE("mock byte_channel delivers on_data posted, never synchronously", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    std::vector<std::byte> received;
    ch.on_data([&](std::span<const std::byte> d) { received.assign(d.begin(), d.end()); });

    const auto payload = bytes_of({0x10, 0x20, 0x30, 0x40});
    ch.inject_inbound(std::span<const std::byte>(payload));

    // The posted-delivery contract: nothing fires until the executor drains.
    REQUIRE(received.empty());

    h.drive();
    REQUIRE(received == payload);
}

TEST_CASE("mock byte_channel injects error exactly once after drain", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    int                  errors = 0;
    plexus::io::io_error seen{};
    ch.on_error(
            [&](plexus::io::io_error e)
            {
                ++errors;
                seen = e;
            });

    ch.inject_error(plexus::io::io_error::broken_pipe);
    REQUIRE(errors == 0);

    h.drive();
    REQUIRE(errors == 1);
    REQUIRE(seen == plexus::io::io_error::broken_pipe);
}

TEST_CASE("mock byte_channel injects close exactly once after drain", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    int closes = 0;
    ch.on_closed([&]() { ++closes; });

    ch.inject_close();
    REQUIRE(closes == 0);

    h.drive();
    REQUIRE(closes == 1);
}

TEST_CASE("mock byte_channel returns the configured remote endpoint", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    ch.set_remote_endpoint({"mock", "node-7"});
    const auto ep = ch.remote_endpoint();

    REQUIRE(ep.scheme == "mock");
    REQUIRE(ep.address == "node-7");
}

TEST_CASE("mock byte_channel advance drives posted inbound across a step", "[testing][mock][fidelity]")
{
    harness h;
    auto    ch = h.make_channel();

    bool fired = false;
    ch.on_data([&](std::span<const std::byte>) { fired = true; });

    const auto payload = bytes_of({0x55});
    ch.inject_inbound(std::span<const std::byte>(payload));
    REQUIRE(!fired);

    // advance(d) crosses time AND drains — the inbound surfaces on the drain.
    h.advance(test_clock::duration{1});
    REQUIRE(fired);
}
