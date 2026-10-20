#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/io_error.h"

#include "plexus/io/detail/drop_event.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using event_log = std::vector<std::string>;

namespace {

constexpr std::array<std::byte, 2> k_payload{std::byte{0x01}, std::byte{0x02}};

struct inproc_pair
{
    inproc_pair()
            : bus()
            , exec(bus)
            , victim(std::make_unique<inproc_channel<>>(exec))
            , peer(std::make_unique<inproc_channel<>>(exec))
    {
        victim->connect_to(peer->local_endpoint());
        peer->connect_to(victim->local_endpoint());
    }

    inproc_bus<> bus;
    inproc_executor<> exec;
    std::unique_ptr<inproc_channel<>> victim;
    std::unique_ptr<inproc_channel<>> peer;
};

}

TEST_CASE("inproc_retire_quiescence closing a channel posts its on_closed exactly once", "[inproc][retire_quiescence][!shouldfail]")
{
    inproc_pair p;
    std::int32_t closes = 0;
    p.victim->on_closed([&closes] { ++closes; });

    p.victim->close();
    REQUIRE(closes == 0);
    p.exec.drain();
    p.victim->close();
    p.exec.drain();
    REQUIRE(closes == 1);
}

TEST_CASE("inproc_retire_quiescence a delivery queued before close is not handed to the consumer", "[inproc][retire_quiescence][!shouldfail]")
{
    inproc_pair p;
    event_log log;
    p.victim->on_data([&log](std::span<const std::byte>) { log.push_back("data"); });
    p.victim->on_drop([&log](const plexus::io::detail::drop_event &) { log.push_back("drop"); });

    p.peer->send(k_payload);
    p.peer.reset();
    p.victim->send(k_payload);
    p.victim->close();
    p.exec.drain();
    REQUIRE(log.empty());
}

TEST_CASE("inproc_retire_quiescence a handler installed after the channel closed and went quiet is still notified", "[inproc][retire_quiescence][!shouldfail]")
{
    inproc_pair p;
    p.victim->close();
    p.exec.drain();

    std::int32_t closes = 0;
    p.victim->on_closed([&closes] { ++closes; });
    REQUIRE(closes == 0);
    p.exec.drain();
    REQUIRE(closes == 1);
}

TEST_CASE("inproc_retire_quiescence a peer's close reaches on_error before the channel's on_closed", "[inproc][retire_quiescence][!shouldfail]")
{
    inproc_pair p;
    event_log log;
    p.victim->on_error([&log](plexus::io::io_error e) { log.push_back(e == plexus::io::io_error::broken_pipe ? "broken_pipe" : "other"); });
    p.victim->on_closed([&log] { log.push_back("closed"); });

    p.peer->close();
    p.exec.drain();
    REQUIRE(log == event_log{"broken_pipe", "closed"});
}
