#include "test_node_close_quiescence_common.h"

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/endpoint_id.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/pump.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

using namespace node_close_fixture;

namespace {

using namespace std::chrono_literals;
using tcp_node = plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport>;

plexus::node_options tcp_opts()
{
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{20ms, 40ms, std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

template<typename Node>
std::size_t sessions_of(Node &node)
{
    std::size_t n = 0;
    node.router().registry().for_each_connected([&n](const plexus::node_id &, auto &) { ++n; });
    return n;
}

void poll_for(::asio::io_context &io, std::chrono::milliseconds span, plexus::publisher<> &pub)
{
    const auto until = std::chrono::steady_clock::now() + span;
    while(std::chrono::steady_clock::now() < until)
    {
        pub.publish(k_payload);
        io.restart();
        io.poll();
    }
}

}

TEST_CASE("node_close a TCP node destroyed inside its on_closed while its peer keeps running is ASan-clean", "[node][node_close]")
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc_a{{}};
    plexus::discovery::static_discovery disc_b{{}};
    plexus::asio::asio_transport ta{io};
    plexus::asio::asio_transport tb{io};
    auto a = std::make_unique<tcp_node>(io, disc_a, make_id(0x0A), ta, tcp_opts());
    tcp_node b{io, disc_b, make_id(0x0B), tb, tcp_opts()};

    a->listen({"tcp", "127.0.0.1:0"});
    const endpoint ep_a{"tcp", "127.0.0.1:" + std::to_string(ta.port())};
    const plexus::node_id slot = plexus::io::endpoint_id(ep_a);
    b.dial(ep_a);
    REQUIRE(plexus::testing::pump_until(io, [&] { return b.router().is_connected(slot) && sessions_of(*a) == 1; }));

    std::int32_t delivered = 0;
    std::int32_t completed = 0;
    std::optional<plexus::subscriber<>> sub;
    sub.emplace(*a, "topic", [&](std::span<const std::byte>) { ++delivered; });
    plexus::publisher<> pub{b, "topic"};
    plexus::procedure<> held{b, "proc", hold_request};
    plexus::caller<> call{*a, "proc"};
    REQUIRE(plexus::testing::pump_until(io,
                                        [&]
                                        {
                                            pub.publish(k_payload);
                                            return delivered > 0;
                                        }));
    call.call(std::span<const std::byte>{}, plexus::call_options{.deadline = 10s}, [&](const auto &) { ++completed; });
    pub.publish(k_payload);

    bool closed = false;
    on_closed_or_fail(*a,
                      [&]
                      {
                          closed = true;
                          sub.reset();
                          a.reset();
                      });
    close_or_fail(*a);
    REQUIRE(plexus::testing::pump_until(io, [&] { return closed; }));
    REQUIRE(a == nullptr);
    REQUIRE(completed == 1);

    poll_for(io, 300ms, pub);
    REQUIRE_FALSE(b.router().is_connected(slot));
    REQUIRE(b.router().attempt_count(slot) >= 1);
}
