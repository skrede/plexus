// Wave-0 failing oracle for the single-connection reconnect driver. It is
// compiled against the planned reconnect driver API (reconnect.h /
// reconnect_config.h) which does NOT exist yet — the headers land with the
// reconnect-driver work and turn this stub green. The full surrender / ceiling /
// fresh-epoch coverage is expanded there; this stub only names the API and asserts
// the refused-dial → schedule_redial contract so the driver is built against a
// failing oracle from the start.

#include "plexus/io/reconnect.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;

TEST_CASE("inproc reconnect: an initial refused dial schedules a re-dial through backoff",
          "[integration][reconnect][inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    // The reconnect driver observes the transport's on_dial_failed and arms a
    // backoff timer to re-dial. With no listener registered, the first dial is
    // refused; the driver must schedule a re-dial (attempt counter advances) rather
    // than surrender immediately.
    plexus::io::reconnect_config cfg{
        .min_delay = std::chrono::milliseconds(10),
        .max_delay = std::chrono::milliseconds(100),
    };
    plexus::io::reconnect<inproc_policy, inproc_transport<>> driver{
        transport, ex, cfg, {"inproc", "svc"}};

    driver.start();           // begins the initial dial
    ex.drain();

    // The refused dial must have scheduled a re-dial, not surrendered.
    REQUIRE(driver.attempt_count() >= 1);
    REQUIRE(!driver.is_surrendered());
}
