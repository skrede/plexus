#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_timer.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

using namespace plexus::asio;

namespace {

// Compile-time data point: the asio Policy satisfies the seam (also asserted in
// asio_policy.h; restated here so the test TU is self-evidently the gate).
static_assert(plexus::Policy<asio_policy>);

}

TEST_CASE("asio_policy constructs its timer and channel from the io_context", "[asio]")
{
    ::asio::io_context io;

    asio_timer   timer(io);
    asio_channel channel(io);

    // The timer is constructible from the executor alone (still a Policy constraint).
    // The channel's executor-alone ctor is now a CONVENIENCE that mints a real
    // default-config channel (the stream::stream_inbound_config defaulted to {}) — NOT
    // a Policy constraint: channel construction is owned by transport_backend. A
    // fresh channel reports an empty remote_endpoint until connected/accepted.
    CHECK(channel.remote_endpoint().scheme == "tcp");
    CHECK(channel.remote_endpoint().address.empty());

    // Exercise the timer's error_code-overload ctor the Policy concept still requires.
    std::error_code ec;
    asio_timer      timer_ec(io, ec);
    CHECK_FALSE(ec);
}

TEST_CASE("asio_policy::post runs a callback under io_context::run", "[asio]")
{
    ::asio::io_context io;

    bool ran = false;
    asio_policy::post(io, [&ran] { ran = true; });

    CHECK_FALSE(ran); // posted, not run inline
    io.run();
    CHECK(ran);
}
