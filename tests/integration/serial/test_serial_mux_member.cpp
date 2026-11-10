// The two-live-transport relay node was unspellable while serial_transport was not mux-composable:
// a relay owning both a serial session and a network session needs node<relay<Policy>, serial, tcp>
// to instantiate. This TU proves the spelling now compiles and constructs from borrowed leaves (the
// consumer-facing shape), and that a serial:// dial through the multiplexer resolves a candidate
// instead of silently no-opping — the misclassified-scheme denial that flipping mux_tier to remote
// closes. It is a compile-and-construct proof; the wire crossing lives in the acceptance oracle.

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/serial_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/node.h"
#include "plexus/policy.h"
#include "plexus/muxify.h"
#include "plexus/node_options.h"
#include "plexus/target_profile.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>
#include <type_traits>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

static_assert(pio::mux_member<pasio::serial_transport>,
              "serial_transport must satisfy mux_member — check channel_type + mux_schemes + mux_tier");
static_assert(pasio::serial_transport::mux_tier == pio::transport_kind::remote,
              "a cabled UART crosses the host boundary: serial classifies remote, agreeing with transport_selector::select");

using relay_serial_node = plexus::node<plexus::relay<pasio::asio_policy>, pasio::serial_transport, pasio::asio_transport>;

static_assert(std::is_same_v<relay_serial_node::engine_policy, plexus::muxify<pasio::asio_policy>>,
              "a two-transport relay node resolves its engine through the muxify policy path");

TEST_CASE("a relay node composes a serial+tcp node from its borrowed leaves", "[integration][mux][node][serial]")
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{{}};
    pasio::serial_transport serial{io};
    pasio::asio_transport remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x2A};
    relay_serial_node node{io, disc, id, serial, remote, plexus::node_options{}};

    SUCCEED("node<relay<asio_policy>, serial_transport, asio_transport> instantiated and constructed");
}

// A serial:// dial through the bare multiplexer must reach the serial member: the resolved candidate
// dispatches to serial_transport::dial, whose bogus device open fails and surfaces on_dial_failed.
// If the scheme resolved to ZERO candidates (the pre-fix silent no-op), on_dial_failed never fires.
TEST_CASE("a serial:// dial resolves a mux candidate instead of no-opping", "[integration][mux][serial]")
{
    ::asio::io_context io;
    pasio::serial_transport serial{io};
    pasio::asio_transport remote{io};

    pio::multiplexing_transport<pasio::serial_transport, pasio::asio_transport> mux{serial, remote};

    bool dial_failed = false;
    mux.on_dial_failed([&](const pio::endpoint &, pio::io_error) { dial_failed = true; });
    mux.dial({"serial", "/nonexistent/plexus-serial-device@115200"});

    REQUIRE(dial_failed);
}
