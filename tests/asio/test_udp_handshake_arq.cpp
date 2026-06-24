#include "test_udp_handshake_arq_common.h"

using namespace udp_handshake_arq_fixture;

TEST_CASE("udp handshake_arq: the session establishes under injected loss via retransmit", "[udp][handshake]")
{
    constexpr int k_iterations = 30;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_ladder};

        bool accepted = false;
        bool dialed   = false;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel>) { accepted = true; });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        // Drop the first datagram each way: the dialer's first hs_request (and/or the
        // first hs_response) is lost, so the ARQ must retransmit to establish.
        lossy_relay relay{io, server.port(), /*drops=*/1};

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel>, const plexus::io::endpoint &) { dialed = true; });
        client.dial({"udp", "127.0.0.1:" + std::to_string(relay.port())});

        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed);
        REQUIRE(accepted);
        REQUIRE(relay.dropped >= 1); // a datagram WAS dropped — the path was genuinely lossy
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp handshake_arq: dropping every datagram surfaces a handshake-timeout abort", "[udp][handshake]")
{
    constexpr int k_iterations = 10;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_ladder};

        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        lossy_relay relay{io, server.port(), /*drops=*/10000}; // drop everything

        std::optional<plexus::io::io_error> dial_fail;
        bool                                dialed = false;
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel>, const plexus::io::endpoint &) { dialed = true; });
        client.on_dial_failed([&](const plexus::io::endpoint &, plexus::io::io_error e) { dial_fail = e; });
        client.dial({"udp", "127.0.0.1:" + std::to_string(relay.port())});

        pump_until(io, [&] { return dial_fail.has_value(); });
        REQUIRE_FALSE(dialed);
        REQUIRE(dial_fail.has_value());
        REQUIRE(*dial_fail == plexus::io::io_error::timed_out);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
