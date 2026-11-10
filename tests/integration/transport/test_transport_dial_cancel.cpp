#include "transport_dial_cancel_common.h"

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <string>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using namespace transport_dial_cancel;

TEST_CASE("transport_dial_cancel: a TCP dial still connecting when its transport closes reports one aborted failure and is never dialed", "[integration][transport][dial_cancel]")
{
    ::asio::io_context io;
    full_backlog peer{io};
    outcome<pasio::asio_channel> seen;
    pasio::asio_transport transport{io};
    seen.record(transport);

    transport.dial(pio::endpoint{"tcp", peer.address()});
    hold_pending(io, seen);
    transport.close();
    drain(io);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}

TEST_CASE("transport_dial_cancel: a UDP dial in its handshake when its transport closes reports one aborted failure and is never dialed", "[integration][transport][udp][dial_cancel]")
{
    ::asio::io_context io;
    silent_udp_peer peer{io};
    outcome<pasio::udp_channel> seen;
    pasio::udp_transport transport{io};
    seen.record(transport);

    transport.dial(pio::endpoint{"udp", peer.address()});
    REQUIRE(run_until(io, [&] { return peer.heard(); }));
    REQUIRE(seen.failures.empty());
    REQUIRE(seen.dialed.empty());
    transport.close();
    drain(io);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}

TEST_CASE("transport_accept_cancel: a TCP accept completed but not yet handled when its transport closes is dropped", "[integration][transport][accept_cancel]")
{
    ::asio::io_context io;
    outcome<pasio::asio_channel> seen;
    pasio::asio_transport transport{io};
    seen.record(transport);
    transport.listen(pio::endpoint{"tcp", "127.0.0.1:0"});
    const ::asio::ip::tcp::endpoint target{::asio::ip::make_address("127.0.0.1"), transport.port()};
    ::asio::ip::tcp::socket first{io};
    ::asio::ip::tcp::socket second{io};
    first.connect(target);
    second.connect(target);

    // Handling the first accept re-arms the accept, which takes the second connection off the backlog at once and
    // queues its completion.
    io.poll_one();
    REQUIRE(seen.accepted.size() == 1);
    transport.close();
    drain(io);

    CHECK(seen.accepted.size() == 1);
    CHECK(closed_by_peer(second));
}
