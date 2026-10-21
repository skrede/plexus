#include "tls_retire_drain_common.h"
#include "transport_dial_cancel_common.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace transport_dial_cancel;

TEST_CASE("transport_dial_cancel: a TLS dial still connecting when its transport closes reports one aborted failure and is never dialed",
          "[integration][transport][tls][dial_cancel][!shouldfail]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    full_backlog peer{io};
    outcome<ptls::tls_channel> seen;
    ptls::tls_transport transport{io, creds.client};
    seen.record(transport);

    transport.dial(pio::endpoint{"tls", peer.address()});
    hold_pending(io, seen);
    transport.close();
    drain(io);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}

TEST_CASE("transport_accept_cancel: a TLS accept completed but not yet handled when its transport closes is dropped", "[integration][transport][tls][accept_cancel][!shouldfail]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    outcome<ptls::tls_channel> seen;
    ptls::tls_transport transport{io, creds.server};
    seen.record(transport);
    transport.listen(pio::endpoint{"tls", "127.0.0.1:0"});
    const ::asio::ip::tcp::endpoint target{::asio::ip::make_address("127.0.0.1"), transport.port()};
    ::asio::ip::tcp::socket first{io};
    ::asio::ip::tcp::socket second{io};
    first.connect(target);
    second.connect(target);

    // Handling the first accept starts its server handshake and re-arms the accept, which takes the second connection
    // off the backlog at once and queues its completion.
    io.poll_one();
    transport.close();
    drain(io);

    CHECK(seen.accepted.empty());
    CHECK(closed_by_peer(first));
    CHECK(closed_by_peer(second));
}
