#include "test_udp_send_burst_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_send_burst_fixture;

TEST_CASE("udp burst: N best_effort datagrams queued in one turn each arrive intact, in order", "[udp][burst][reproducibility]")
{
    constexpr int k_iterations = 100;
    constexpr int k_burst      = 8;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pair_fixture h;
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        // Issue the whole burst in ONE turn: NO io.poll() between sends, so every
        // datagram's bytes are still queued when the next send overwrites the channel's
        // send scratch. Distinct, size-varying payloads so a reused-scratch sink would
        // transmit the LAST payload (or torn bytes / a reallocated buffer) for the earlier
        // legs — the receiver would then NOT see each distinct payload exactly once.
        std::vector<std::string> sent;
        for(int i = 0; i < k_burst; ++i)
        {
            std::string p = "burst-" + std::to_string(iter) + "-" + std::to_string(i) + std::string(static_cast<std::size_t>(i) * 7, 'x'); // size varies per leg
            sent.push_back(p);
            h.dialed->send(bytes_of(p)); // queued; the scratch is reused on the next iteration
        }

        pump_until(h.io, [&] { return h.received.size() == sent.size(); });
        REQUIRE(h.received == sent); // each distinct payload, intact, in publish order
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp burst: two channels over one server each send in the same turn without overlap", "[udp][burst][reproducibility]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // One listening server, TWO dialing clients (each its own transport/server), so
        // two DISTINCT accepted channels share the listening server's ONE outbound socket
        // for their handshake responses and any sends. The interest is the listening
        // server's send path: it must keep each in-flight datagram alive.
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client_a{io, pasio::udp_channel::default_max_payload, fast_hs};
        pasio::udp_transport client_b{io, pasio::udp_channel::default_max_payload, fast_hs};

        std::vector<std::unique_ptr<pasio::udp_channel>> accepted;
        std::unique_ptr<pasio::udp_channel> dia_a, dia_b;
        std::vector<std::string> dialer_seen; // every payload the two dialers receive
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted.push_back(std::move(ch)); });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        const std::string ep = "udp", host = "127.0.0.1:" + std::to_string(server.port());
        client_a.on_dialed(
                [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                {
                    dia_a = std::move(ch);
                    dia_a->on_data([&](std::span<const std::byte> b) { dialer_seen.push_back(str_of(b)); });
                });
        client_b.on_dialed(
                [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                {
                    dia_b = std::move(ch);
                    dia_b->on_data([&](std::span<const std::byte> b) { dialer_seen.push_back(str_of(b)); });
                });
        client_a.dial({ep, host});
        client_b.dial({ep, host});
        pump_until(io, [&] { return accepted.size() == 2 && dia_a && dia_b; });
        REQUIRE(accepted.size() == 2);

        // The two accepted channels (sharing the listening server's ONE outbound socket)
        // each send to their OWN dialer in the same turn, so the server's outbound queue
        // holds two distinct in-flight datagrams at once. Distinct, size-varying payloads:
        // a reused scratch would cross them or transmit torn bytes for the earlier leg.
        const std::string pa = "to-peer-a-" + std::to_string(iter);
        const std::string pb = "to-peer-b-" + std::to_string(iter) + "-longer-tail";
        accepted[0]->send(bytes_of(pa));
        accepted[1]->send(bytes_of(pb));

        pump_until(io, [&] { return dialer_seen.size() == 2; });
        // Both payloads arrive intact across the two dialers — neither overwritten by the
        // other's in-flight datagram (order across peers is not guaranteed; the SET is).
        std::sort(dialer_seen.begin(), dialer_seen.end());
        std::vector<std::string> expect{pa, pb};
        std::sort(expect.begin(), expect.end());
        REQUIRE(dialer_seen == expect);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
