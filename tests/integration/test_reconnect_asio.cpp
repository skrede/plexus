// Wave-0 failing oracle for the gated real-TCP reconnect driver. It is compiled
// against the planned reconnect driver API (reconnect.h / reconnect_config.h)
// which does NOT exist yet — the headers land with the reconnect-driver work and
// turn this stub green. The full drop-mid-session → re-dial → re-handshake
// coverage over real TCP is expanded there; this stub only names the API and
// asserts the established-drop → on_channel_dropped contract so the driver is
// built against a failing oracle from the start.

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/reconnect.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <cstdint>

namespace pasio = plexus::asio;

TEST_CASE("asio reconnect: an established session whose channel drops re-dials over real TCP",
          "[integration][reconnect][asio]")
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};

    plexus::io::reconnect_config cfg{
        .min_delay = std::chrono::milliseconds(10),
        .max_delay = std::chrono::milliseconds(100),
    };
    plexus::io::reconnect<pasio::asio_policy, pasio::asio_transport> driver{
        transport, io, cfg, {"tcp", "127.0.0.1:0"}};

    // Once an established session's channel drops, the driver's on_channel_dropped
    // tears down the dead incarnation and schedules a re-dial. This stub names the
    // API surface; the full real-TCP drop → re-dial → re-handshake proof is
    // expanded by the reconnect-driver work.
    driver.on_channel_dropped();

    REQUIRE(driver.attempt_count() >= 1);
    REQUIRE(!driver.is_surrendered());
}
