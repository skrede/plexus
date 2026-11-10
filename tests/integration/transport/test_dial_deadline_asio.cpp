#include "transport_dial_cancel_common.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <optional>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using namespace transport_dial_cancel;
using namespace std::chrono_literals;

namespace {

using tcp_node = plexus::node<pasio::asio_policy, pasio::asio_transport>;

template<typename Ready>
bool run_within(::asio::io_context &io, std::chrono::steady_clock::duration bound, Ready ready)
{
    const auto until = std::chrono::steady_clock::now() + bound;
    while(!ready() && std::chrono::steady_clock::now() < until)
    {
        io.restart();
        io.run_for(5ms);
    }
    return ready();
}

}

TEST_CASE("dial deadline: a node dialing a host that never answers counts a failed attempt within its dial timeout", "[integration][dial_deadline]")
{
    ::asio::io_context io;
    full_backlog peer{io};
    plexus::discovery::static_discovery disc{{}};
    pasio::asio_transport transport{io};
    plexus::node_options opts;
    opts.reconnect   = pio::reconnect_config{20ms, 40ms, std::nullopt, std::nullopt};
    opts.redial_seed = 0xD1A1u;
    tcp_node node{io, disc, "dialer", transport, opts};
    const pio::endpoint ep{"tcp", peer.address()};

    const auto start = std::chrono::steady_clock::now();
    node.dial(ep);
    const bool counted = run_within(io, 8s, [&] { return node.router().attempt_count(pio::endpoint_id(ep)) >= 1; });
    const auto elapsed = std::chrono::steady_clock::now() - start;
    node.close();
    drain(io);

    INFO("elapsed ms: " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    CHECK(counted);
    CHECK(elapsed < 8s);
}

TEST_CASE("dial deadline: a TCP dial that never completes fails with timed_out within the dial timeout", "[integration][dial_deadline]")
{
    ::asio::io_context io;
    full_backlog peer{io};
    outcome<pasio::asio_channel> seen;
    pasio::asio_transport transport{io};
    seen.record(transport);
    transport.set_dial_timeout(200ms);

    const auto start = std::chrono::steady_clock::now();
    transport.dial(pio::endpoint{"tcp", peer.address()});
    hold_pending(io, seen);
    REQUIRE(run_until(io, [&] { return !seen.failures.empty(); }));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    transport.close();
    drain(io);

    INFO("ms to timed_out: " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    CHECK(elapsed >= 200ms);
    CHECK(elapsed < 2s);
    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::timed_out);
}

TEST_CASE("dial deadline: a TCP dial that completes before the deadline is untouched", "[integration][dial_deadline]")
{
    ::asio::io_context io;
    outcome<pasio::asio_channel> served;
    pasio::asio_transport server{io};
    served.record(server);
    server.listen(pio::endpoint{"tcp", "127.0.0.1:0"});
    outcome<pasio::asio_channel> seen;
    pasio::asio_transport transport{io};
    seen.record(transport);
    transport.set_dial_timeout(200ms);

    transport.dial(pio::endpoint{"tcp", "127.0.0.1:" + std::to_string(server.port())});
    REQUIRE(run_until(io, [&] { return seen.dialed.size() == 1 && served.accepted.size() == 1; }));
    bool closed = false;
    seen.dialed.front()->on_closed([&closed] { closed = true; });
    io.restart();
    io.run_for(400ms);

    CHECK(seen.failures.empty());
    CHECK_FALSE(closed);
    CHECK(seen.dialed.front()->socket().is_open());
    seen.dialed.front()->close();
    served.accepted.front()->close();
    transport.close();
    server.close();
    drain(io);
}

TEST_CASE("dial deadline: closing a transport with an armed dial reports one aborted failure, not a timeout", "[integration][dial_deadline]")
{
    ::asio::io_context io;
    full_backlog peer{io};
    outcome<pasio::asio_channel> seen;
    pasio::asio_transport transport{io};
    seen.record(transport);
    transport.set_dial_timeout(300ms);

    transport.dial(pio::endpoint{"tcp", peer.address()});
    hold_pending(io, seen);
    transport.close();
    drain(io);
    io.restart();
    io.run_for(300ms);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}
