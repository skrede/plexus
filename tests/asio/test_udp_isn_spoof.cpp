#include "test_udp_isn_common.h"

using namespace udp_isn_fixture;

TEST_CASE("udp isn: a spoofed seq=0 reliable-data segment is rejected on a non-zero-ISN plaintext "
          "udpr session",
          "[udp][isn][spoof]")
{
    constexpr int k_iterations = 50;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload, pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string>            delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(accepted != nullptr);

        // An off-path attacker forges a [kind=reliable-data][seq=0] data segment that
        // reaches the receiver as if from the trusted peer (the source endpoint is NOT
        // trusted as identity — a spoofed-source datagram demuxes to this established
        // channel). deliver_inbound is exactly the post-demux entry. With the negotiated ISN
        // non-zero, seq=0 sits OUTSIDE the receiver's in-order window, so the forged payload
        // is never delivered.
        std::vector<std::byte> inner;
        wire::encode_udp_segment_into(inner, bytes_of("FORGED"));
        std::vector<std::byte> spoof;
        wire::wrap_udp_into(spoof, wire::udp_envelope_kind::reliable_arq, 0, inner);
        accepted->deliver_inbound(spoof);

        // Drain any posted work so a (mis)delivery would have surfaced, then assert none did.
        for(int i = 0; i < 64; ++i)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
        REQUIRE(delivered.empty()); // the forged seq=0 was rejected (out of window)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp isn: per-session ISNs drawn from OS entropy are high-entropy, not a reconstructible "
          "stream",
          "[udp][isn][entropy]")
{
    // The dialer's per-session ISN is now drawn directly from std::random_device (the OS
    // CSPRNG) instead of a single setup-time-seeded std::mt19937 whose stream was
    // reconstructible from ISNs echoed in cleartext handshakes. Sniff the request handshakes
    // from many INDEPENDENT client transports (each draws its own ISN) at a raw udp_server
    // and assert the collected ISNs are well-distributed: never 0, overwhelmingly distinct,
    // and no fixed sequential step (the signatures a low-entropy / reconstructible source
    // would leave). This is a statistical property, so the bar is loose enough to never flake
    // on a sound CSPRNG yet tight enough to catch a constant / counter / single-seed stream.
    namespace iod            = plexus::datagram::detail;
    constexpr int k_sessions = 64;

    ::asio::io_context         io;
    pasio::udp_server          sniffer{io};
    std::vector<std::uint16_t> isns;
    sniffer.on_datagram(
            [&](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b)
            {
                if(auto hs = iod::decode_handshake(b); hs && hs->type == iod::udp_hs_type::request)
                    isns.push_back(hs->initial_seq);
            });
    sniffer.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
    pump_until(io, [&] { return sniffer.port() != 0; });

    std::vector<std::unique_ptr<pasio::udp_transport>> clients;
    for(int i = 0; i < k_sessions; ++i)
    {
        auto c = std::make_unique<pasio::udp_transport>(io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq());
        c->dial({"udpr", "127.0.0.1:" + std::to_string(sniffer.port())});
        clients.push_back(std::move(c));
    }
    pump_until(io, [&] { return isns.size() >= static_cast<std::size_t>(k_sessions); });

    REQUIRE(isns.size() >= static_cast<std::size_t>(k_sessions));
    isns.resize(k_sessions);

    for(auto v : isns)
        REQUIRE(v != 0); // 0 is reserved for the legacy default

    std::vector<std::uint16_t> sorted = isns;
    std::sort(sorted.begin(), sorted.end());
    const auto distinct = static_cast<std::size_t>(std::distance(sorted.begin(), std::unique(sorted.begin(), sorted.end())));
    REQUIRE(distinct >= static_cast<std::size_t>(k_sessions - 2)); // ~no collisions over a 16-bit space

    // No fixed step: a counter / single-stride sequence would show one dominant delta. Across
    // arrival order the consecutive deltas must vary (a CSPRNG yields a spread of deltas).
    std::size_t      distinct_deltas = 0;
    std::vector<int> deltas;
    for(std::size_t i = 1; i < isns.size(); ++i)
        deltas.push_back(static_cast<int>(isns[i]) - static_cast<int>(isns[i - 1]));
    std::sort(deltas.begin(), deltas.end());
    distinct_deltas = static_cast<std::size_t>(std::distance(deltas.begin(), std::unique(deltas.begin(), deltas.end())));
    REQUIRE(distinct_deltas > deltas.size() / 2); // not a single repeating stride
}
