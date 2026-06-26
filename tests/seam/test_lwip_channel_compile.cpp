// Host compile-check for the constrained-target lwIP byte_channel: this translation unit's
// sole job off-target is to force lwip_channel.h through the host compiler so its in-header
// byte_channel proof and the stream_inbound / send_queue reuse are validated by the PC build
// (the lwIP socket itself is ESP_PLATFORM-gated and never reached here; the host's real driving
// fixture binds a genuine POSIX-TCP stream_socket and lands alongside the transport).

#include "plexus/freertos/lwip_channel.h"

#include <catch2/catch_test_macros.hpp>

// The explicit witness at the test site (mirrors the gate in the header).
static_assert(plexus::io::byte_channel<plexus::freertos::lwip_channel<plexus::freertos::detail::null_stream_socket>>,
              "lwip_channel must satisfy byte_channel at the seam-test site");

TEST_CASE("lwip_channel constructs over a null stream_socket and exposes the byte_channel seam", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    plexus::freertos::lwip_channel<plexus::freertos::detail::null_stream_socket> ch{plexus::freertos::detail::null_stream_socket{}, ex, plexus::io::endpoint{"tcp", "127.0.0.1:9000"}};

    REQUIRE(ch.remote_endpoint() == plexus::io::endpoint{"tcp", "127.0.0.1:9000"});
    ch.poll(); // a null socket yields no bytes and is not closed — a clean no-op
    ch.close();
}
