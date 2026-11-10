#include "tls_retire_drain_common.h"
#include "transport_dial_cancel_common.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace transport_dial_cancel;

TEST_CASE("transport_dial_cancel: a DTLS dial in its handshake when its transport closes reports one aborted failure and is never dialed", "[integration][transport][dtls][dial_cancel]")
{
    tls_retire_drain::mutual_dtls_credentials creds;
    ::asio::io_context io;
    silent_udp_peer peer{io};
    outcome<ptls::dtls_channel> seen;
    ptls::dtls_transport transport{io, creds.client};
    seen.record(transport);

    transport.dial(pio::endpoint{"dtls", peer.address()});
    REQUIRE(run_until(io, [&] { return peer.heard(); }));
    REQUIRE(seen.failures.empty());
    REQUIRE(seen.dialed.empty());
    transport.close();
    drain(io);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}
