#include "test_outofbox_message_ceiling_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace outofbox_ceiling_fixture;

namespace {

// An ARQ pacing config (a flow-control knob, NOT a size knob): a generous window + quick
// retransmit drive the 8 MiB message through the reliable ARQ fast enough to complete within
// the default per-message reassembly reclaim window, while the in-flight batch stays under the
// raised kernel socket buffers. The per-message size ceiling, write/back-pressure caps, and
// reassembly budget all stay at the shipped defaults — this only governs HOW FAST fragments
// leave, not how large a message may be. A generous retransmit budget covers loopback loss.
inline pio::detail::udp_arq_config paced_arq()
{
    return pio::detail::udp_arq_config{.window         = 1024,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{160},
                                       .max_retransmit = 40};
}

}

TEST_CASE("outofbox: an 8 MiB message round-trips over udpr at shipped defaults, looped",
          "[outofbox][envelope8]")
{
    // The datagram leg at shipped defaults: the per-channel back-pressure cap is the OLD
    // 4 MiB default, now floored at the 8 MiB shipped ceiling — so the message's full
    // fragment backlog parks admissibly with NO backpressure_bytes bump. The reassembly
    // budget default (16 MiB) holds the 8 MiB reassembled message. Only the ARQ pacing
    // window and socket buffers (flow-control, not size) are set so the burst is buffered.
    constexpr std::size_t budget       = 8u * 1024u; // ~1024 fragments at the ceiling
    constexpr std::size_t k_socket_buf = 4u * 1024u * 1024u;
    // The per-message reassembly reclaim window is a LIVENESS knob (it reclaims a genuinely
    // stalled partial), NOT a size knob: an honest multi-megabyte loopback transfer legitimately
    // runs past the 5 s default, so it is extended here exactly as a flow-control concern. The
    // SIZE authorities (ceiling, reassembly budget, back-pressure cap) stay at shipped defaults.
    constexpr ms reassembly_timeout{60000};
    const auto   payload = ramp_payload(k_shipped_ceiling);

    using demux = pasio::detail::udp_inbound_demux;

    constexpr int iterations = 2;
    int           proven     = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        // global_default / reassembly_budget / backpressure_bytes are passed at their SHIPPED
        // DEFAULTS (the proof that the shipped caps hold an 8 MiB message); only the kernel
        // socket buffers and the reassembly reclaim window are raised (flow-control / liveness).
        pasio::udp_transport server{io,
                                    budget,
                                    pasio::udp_transport::arq_type::default_ladder,
                                    paced_arq(),
                                    pio::congestion::block,
                                    demux::default_max_peers,
                                    k_socket_buf,
                                    k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes,
                                    pio::global_default_max_message_bytes,
                                    pio::reassembly_memory_budget,
                                    pasio::udp_channel::default_backpressure_bytes,
                                    reassembly_timeout};
        pasio::udp_transport client{
                io,
                budget,
                pasio::udp_transport::arq_type::schedule{ms{20}, ms{40}, ms{80}},
                paced_arq(),
                pio::congestion::block,
                demux::default_max_peers,
                k_socket_buf,
                k_socket_buf,
                pasio::udp_server::default_send_queue_bytes,
                pio::global_default_max_message_bytes,
                pio::reassembly_memory_budget,
                pasio::udp_channel::default_backpressure_bytes,
                reassembly_timeout};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { got.emplace_back(b.begin(), b.end()); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        dialed->send(payload);
        pump_until(io, [&] { return !got.empty(); }, ms{40000});
        REQUIRE(got.size() == 1);
        REQUIRE(equal_bytes(got.front(),
                            payload)); // byte-equal at the shipped ceiling, default caps
        ++proven;
    }
    REQUIRE(proven == iterations);
}
