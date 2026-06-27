#include "plexus/freertos/detail/lwip_endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>

namespace {

namespace pfd = plexus::freertos::detail;

// The endpoint_type contract the discovery template calls but the datagram_socket concept does NOT
// check (the false-green trap): bind_endpoint() sets the port through .port(uint16) and on inbound
// reads the source host through from.address().to_string(). Forcing the wrapper through the host
// compiler proves the shape without a board — the socket leaf itself is ESP_PLATFORM-gated.
TEST_CASE("lwip_endpoint records the bound port through the discovery setter", "[discovery][seam]")
{
    pfd::lwip_endpoint ep{};
    ep.port(7447);
    REQUIRE(ep.port() == 7447);
}

TEST_CASE("lwip_endpoint yields the bare source host from address().to_string()", "[discovery][seam]")
{
    const std::string host = "192.168.88.42";
    pfd::lwip_endpoint from{host, 7447};
    REQUIRE(from.address().to_string() == host);
}

}
