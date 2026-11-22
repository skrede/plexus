#include "tls_retire_drain_common.h"

#include "plexus/tls/tls_listener.h"
#include "plexus/tls/tls_transport.h"

#include "plexus/io/congestion.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <stdexcept>

TEST_CASE("tls congestion: a TLS transport or listener configured to drop its oldest frames is refused when built", "[tls][congestion]")
{
    const tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    CHECK_THROWS_AS(plexus::tls::tls_transport(io, creds.server, {}, true, plexus::io::congestion::drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(plexus::tls::tls_listener(io, creds.server, {}, true, plexus::io::congestion::drop_oldest), std::runtime_error);
}
