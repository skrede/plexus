#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/discovery/static_discovery.h"
#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>

using namespace plexus::inproc;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Compile-time data point: the inproc Policy satisfies the seam (also asserted
// in inproc_policy.h; restated here so the test TU is self-evidently the gate).
static_assert(plexus::Policy<inproc_policy>);

}

TEST_CASE("inproc executor runs posted callbacks in order and drains to quiescence", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);

    std::vector<int> order;
    ex.post([&] { order.push_back(1); });
    ex.post([&] { order.push_back(2); });
    ex.post([&] { order.push_back(3); });

    REQUIRE(order.empty());           // nothing runs until stepped
    ex.drain();
    REQUIRE(order == std::vector<int>{1, 2, 3});
    REQUIRE_FALSE(ex.step());         // quiescent: step() returns false
}

TEST_CASE("inproc channel delivers bytes only after drain, never synchronously", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> a(ex);
    inproc_channel<> b(ex);
    a.connect_to(b.local_endpoint());
    b.connect_to(a.local_endpoint());

    std::string received;
    b.on_data([&](std::span<const std::byte> d) {
        received.assign(reinterpret_cast<const char *>(d.data()), d.size());
    });

    a.send(as_bytes(std::string{"hello"}));
    REQUIRE(received.empty());        // not delivered synchronously from send()

    ex.drain();
    REQUIRE(received == "hello");     // delivered only via the step-loop
}

TEST_CASE("inproc channel close notifies the partner via the step-loop", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> a(ex);
    inproc_channel<> b(ex);
    a.connect_to(b.local_endpoint());
    b.connect_to(a.local_endpoint());

    bool closed = false;
    plexus::io::io_error err = plexus::io::io_error::unknown;
    b.on_closed([&] { closed = true; });
    b.on_error([&](plexus::io::io_error e) { err = e; });

    a.close();
    REQUIRE_FALSE(closed);            // not delivered synchronously
    ex.drain();
    REQUIRE(closed);
    REQUIRE(err == plexus::io::io_error::broken_pipe);
}

TEST_CASE("static_discovery resolves a seeded name to its endpoint", "[inproc]")
{
    using namespace plexus::discovery;
    plexus::io::endpoint ep{"inproc", "node-a"};
    static_discovery disc(std::vector<service_info>{{"alpha", ep}});

    std::vector<service_info> resolved;
    disc.browse([&](const service_info &s) { resolved.push_back(s); });

    REQUIRE(resolved.size() == 1);
    REQUIRE(resolved[0].name == "alpha");
    REQUIRE(resolved[0].endpoint == ep);
}

TEST_CASE("null_logger is usable through a logger& and is a no-op", "[inproc]")
{
    plexus::log::null_logger sink;
    plexus::log::logger &as_base = sink;
    as_base.warn("dropped on the floor");   // compiles, no-op, no observable effect
    SUCCEED("null_logger warn-and-drop seam exists");
}

TEST_CASE("alloc_counter snapshots a global new/delete delta", "[inproc]")
{
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    volatile auto *p = new int(7);
    const auto after_alloc = plexus::testing::alloc_count();
    delete p;
    REQUIRE(after_alloc > before);          // the override counted the allocation
}
