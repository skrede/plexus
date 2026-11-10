#include "tls_retire_drain_common.h"
#include "transport_dial_cancel_common.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <string>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace transport_dial_cancel;
using namespace std::chrono_literals;

namespace {

std::chrono::milliseconds since(std::chrono::steady_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
}

}

TEST_CASE("dial deadline: a TLS dial whose connect never completes fails with timed_out", "[integration][dial_deadline][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    full_backlog peer{io};
    outcome<ptls::tls_channel> seen;
    ptls::tls_transport transport{io, creds.client};
    seen.record(transport);
    transport.set_dial_timeout(200ms);

    const auto start = std::chrono::steady_clock::now();
    transport.dial(pio::endpoint{"tls", peer.address()});
    hold_pending(io, seen);
    REQUIRE(run_until(io, [&] { return !seen.failures.empty(); }));
    const auto elapsed = since(start);
    transport.close();
    drain(io);

    INFO("ms to timed_out: " << elapsed.count());
    CHECK(elapsed >= 200ms);
    CHECK(elapsed < 2s);
    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::timed_out);
}

TEST_CASE("dial deadline: a TLS dial whose handshake never completes fails with timed_out", "[integration][dial_deadline][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    silent_tcp_peer peer{io};
    outcome<ptls::tls_channel> seen;
    ptls::tls_transport transport{io, creds.client};
    seen.record(transport);
    transport.set_dial_timeout(200ms);

    const auto start = std::chrono::steady_clock::now();
    transport.dial(pio::endpoint{"tls", peer.address()});
    REQUIRE(run_until(io, [&] { return !seen.failures.empty(); }));
    const auto elapsed = since(start);
    REQUIRE(peer.heard());
    drain(io);
    peer.accepted.close();
    drain(io);
    transport.close();
    drain(io);

    INFO("ms to timed_out: " << elapsed.count());
    CHECK(elapsed >= 200ms);
    CHECK(elapsed < 2s);
    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::timed_out);
}

TEST_CASE("dial deadline: a TLS dial that completes before the deadline is untouched", "[integration][dial_deadline][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    outcome<ptls::tls_channel> served;
    ptls::tls_transport server{io, creds.server};
    served.record(server);
    server.listen(pio::endpoint{"tls", "127.0.0.1:0"});
    outcome<ptls::tls_channel> seen;
    ptls::tls_transport transport{io, creds.client};
    seen.record(transport);
    transport.set_dial_timeout(1s);

    const auto start = std::chrono::steady_clock::now();
    transport.dial(pio::endpoint{"tls", "127.0.0.1:" + std::to_string(server.port())});
    REQUIRE(run_until(io, [&] { return seen.dialed.size() == 1; }));
    bool closed = false;
    seen.dialed.front()->on_closed([&closed] { closed = true; });
    while(since(start) < 1200ms)
    {
        io.restart();
        io.run_for(10ms);
    }

    CHECK(seen.failures.empty());
    CHECK_FALSE(closed);
    CHECK(seen.dialed.front()->socket().is_open());
    seen.dialed.front()->close();
    for(auto &ch : served.accepted)
        ch->close();
    transport.close();
    server.close();
    drain(io);
}
