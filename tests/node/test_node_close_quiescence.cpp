#include "test_node_close_quiescence_common.h"

#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/endpoint.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

using namespace node_close_fixture;
using namespace std::chrono_literals;

TEST_CASE("node_close a closed node fires on_closed once, after every edge it posted, and its peer sees the session drop", "[node][node_close][!shouldfail]")
{
    edge_log log;
    close_net net;
    net.a.router().add_observer(log);
    net.connect();
    std::optional<plexus::subscriber<>> sub;
    sub.emplace(net.a, "topic", [](std::span<const std::byte>) {});
    net.ex.drain();

    on_closed_or_fail(net.a,
                      [&]
                      {
                          log.entries.emplace_back("closed");
                          sub.reset();
                      });
    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(log.count("closed") == 1);
    REQUIRE(log.count("disconnected") == 1);
    REQUIRE(log.ends_with("closed"));
    REQUIRE(net.b_sessions() == 0);
}

TEST_CASE("node_close a call pending at close completes once with a failure before on_closed", "[node][node_close][!shouldfail]")
{
    edge_log log;
    close_net net;
    net.connect();
    plexus::procedure<> held{net.b, "proc", hold_request};
    plexus::caller<> call{net.a, "proc"};
    net.ex.drain();
    call.call(std::span<const std::byte>{}, plexus::call_options{.deadline = 10s}, [&](const auto &result) { log.entries.emplace_back(result ? "reply" : "failed"); });
    net.ex.drain();
    REQUIRE(net.a.router().procedures().outstanding() == 1);

    on_closed_or_fail(net.a, [&] { log.entries.emplace_back("closed"); });
    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(log.count("failed") == 1);
    REQUIRE(log.count("closed") == 1);
    REQUIRE(log.ends_with("closed"));
}

TEST_CASE("node_close a handler installed after the node settled is posted once and a delivered notification never repeats", "[node][node_close][!shouldfail]")
{
    close_net net;
    net.connect();
    close_or_fail(net.a);
    net.ex.drain();

    std::int32_t first  = 0;
    std::int32_t second = 0;
    on_closed_or_fail(net.a, [&] { ++first; });
    REQUIRE(first == 0);
    net.ex.drain();
    REQUIRE(first == 1);

    on_closed_or_fail(net.a, [&] { ++second; });
    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(first == 1);
    REQUIRE(second == 0);
}

TEST_CASE("node_close a node closed from inside its own subscriber callback settles once", "[node][node_close][!shouldfail]")
{
    close_net net;
    net.connect();
    std::int32_t delivered = 0;
    std::int32_t closed    = 0;
    plexus::subscriber<> sub{net.a, "topic", [&](std::span<const std::byte>)
                             {
                                 ++delivered;
                                 close_or_fail(net.a);
                             }};
    plexus::publisher<> pub{net.b, "topic"};
    net.ex.drain();

    on_closed_or_fail(net.a, [&] { ++closed; });
    pub.publish(k_payload);
    pub.publish(k_payload);
    net.ex.drain();
    REQUIRE(delivered == 1);
    REQUIRE(closed == 1);
}

TEST_CASE("node_close a closed node's on_closed waits for its liveliness tick and its pending call deadline", "[node][node_close][!shouldfail]")
{
    edge_log log;
    close_net net;
    net.connect();
    const plexus::node_id stale = make_id(0x5A);
    auto record = [&](plexus::wire::rpc_status status, std::span<const std::byte>) { log.entries.emplace_back(status == plexus::wire::rpc_status::canceled ? "canceled" : "expired"); };
    net.a.router().call(make_id(0x5B), "proc", {}, record, 100ms);
    arm_upgrade_retry(net.a);
    net.ex.drain();
    REQUIRE(net.a.router().procedures().outstanding() == 1);

    on_closed_or_fail(net.a, [&] { log.entries.emplace_back("closed"); });
    net.a.router().note_peer(stale, endpoint{"inproc", "host-s:7000"}, 0);
    const std::uint64_t declines = transient_declines(net.a);
    REQUIRE(declines > 0);
    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(log.count("canceled") == 1);
    REQUIRE(log.ends_with("closed"));

    net.run_for(300ms);
    REQUIRE(log.count("closed") == 1);
    REQUIRE(log.ends_with("closed"));
    REQUIRE(net.a.router().known().lookup(stale).has_value());
    REQUIRE(transient_declines(net.a) == declines);
}

TEST_CASE("node_close a node destroyed inside its on_closed leaves its peer running", "[node][node_close][!shouldfail]")
{
    owned_net net;
    plexus::procedure<> held{net.b, "proc", hold_request};
    plexus::publisher<> pub{net.b, "topic"};
    std::optional<plexus::subscriber<>> sub;
    sub.emplace(*net.a, "topic", [](std::span<const std::byte>) {});
    plexus::caller<> call{*net.a, "proc"};
    arm_upgrade_retry(*net.a);
    net.ex.drain();
    call.call(std::span<const std::byte>{}, plexus::call_options{.deadline = 10s}, [](const auto &) {});
    pub.publish(k_payload);

    bool closed = false;
    on_closed_or_fail(*net.a,
                      [&]
                      {
                          closed = true;
                          sub.reset();
                          net.a.reset();
                      });
    close_or_fail(*net.a);
    net.ex.drain();
    REQUIRE(closed);
    REQUIRE_FALSE(net.a.has_value());

    const auto until = std::chrono::steady_clock::now() + 300ms;
    while(std::chrono::steady_clock::now() < until)
    {
        pub.publish(k_payload);
        net.ex.step();
    }
    std::size_t sessions = 0;
    net.b.router().registry().for_each_connected([&sessions](const plexus::node_id &, auto &) { ++sessions; });
    REQUIRE(sessions == 0);
}
