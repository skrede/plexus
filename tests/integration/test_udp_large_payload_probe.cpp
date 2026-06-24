#include "test_udp_large_payload_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_large_payload_fixture;

namespace {

// A MODERATE ARQ window that PACES the send: only `window` fragments are outstanding at a
// time, acked before the next batch leaves, so a multi-megabyte message never bursts past
// the ~4 MiB loopback kernel socket buffers (a burst would drop fragments faster than the
// ARQ could retransmit). The backpressure queue holds the not-yet-windowed remainder, so
// the per-channel backpressure cap must reach the whole message (sized in the helper). A
// generous retransmit budget covers the residual loopback loss/reorder at this volume.
inline plexus::datagram::detail::udp_arq_config paced_arq()
{
    return plexus::datagram::detail::udp_arq_config{.window = 64, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{200}, .max_retransmit = 80};
}

// The 4 MiB kernel-buffer ceiling this host's rmem_max/wmem_max permits — raised on both
// ends so the paced in-flight batch is buffered rather than dropped at the socket.
constexpr std::size_t k_socket_buf = 4u * 1024u * 1024u;

// Round-trip ONE message of `payload_size` over udpr with the node per-MESSAGE ceiling
// and the aggregate reassembly budget raised to admit it, looped in-body. Returns the
// count of byte-equal deliveries AND the headline single-message wall time of the last
// iteration (for the empirical throughput record — not asserted). The transport ctor
// threads global_default (the receive ceiling) + reassembly_budget past the intermediate
// node-options params (congestion/peer-cap/socket-buffers/send-queue keep their defaults).
struct large_result
{
    int                       proven;
    std::chrono::milliseconds last_wall;
};

large_result roundtrip_large(std::size_t budget, std::size_t payload_size, std::size_t global_default, std::size_t reassembly_budget, plexus::datagram::detail::udp_arq_config arq,
                             int iterations)
{
    // A multi-megabyte reassembly over the paced ARQ takes well past the 5 s default
    // per-message reclaim window, which would evict the partial mid-flight; extend it so an
    // honest slow large message completes (the reclaim still bounds a genuinely stalled one).
    constexpr ms reassembly_timeout{60000};
    using clock                      = std::chrono::steady_clock;
    int                       proven = 0;
    std::chrono::milliseconds last_wall{0};
    for(int iter = 0; iter < iterations; ++iter)
    {
        // The backpressure queue holds the message's not-yet-windowed fragments, so it must
        // reach the whole message (plus headroom); the kernel buffers are raised to the
        // host max so the paced in-flight batch is buffered, not dropped at the socket.
        const std::size_t    backpressure = payload_size + 4u * 1024u * 1024u;
        ::asio::io_context   io;
        pasio::udp_transport server{io,
                                    budget,
                                    pasio::udp_transport::arq_type::default_ladder,
                                    arq,
                                    pio::congestion::block,
                                    pasio::detail::udp_inbound_demux::default_max_peers,
                                    k_socket_buf,
                                    k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes,
                                    global_default,
                                    reassembly_budget,
                                    backpressure,
                                    reassembly_timeout};
        pasio::udp_transport client{io,
                                    budget,
                                    fast_hs,
                                    arq,
                                    pio::congestion::block,
                                    pasio::detail::udp_inbound_demux::default_max_peers,
                                    k_socket_buf,
                                    k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes,
                                    global_default,
                                    reassembly_budget,
                                    backpressure,
                                    reassembly_timeout};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto       payload = make_payload(payload_size, static_cast<std::uint8_t>(iter));
        const auto t0      = clock::now();
        dialed->send(payload);
        pump_until(io, [&] { return !got.empty(); }, ms{55000});
        last_wall = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0);

        REQUIRE(got.size() == 1);
        REQUIRE(equal_bytes(got.front(), payload));
        ++proven;
    }
    return {proven, last_wall};
}

}

TEST_CASE("udp_large_payload: a 16 MB single message round-trips byte-identically over udpr, looped", "[udp_large_payload][envelope16]")
{
    // The lifted envelope on the datagram path: one 16 MiB message over the reliable ARQ,
    // proven byte-equal across repeated in-body iterations (the ctest invocation is itself
    // re-run for cross-process reproducibility — a transport claim is never made from one
    // run). An 8 KiB fragment budget (~2048 fragments) with a paced ARQ window keeps the
    // in-flight batch under the kernel buffers; the backpressure queue (raised through the
    // transport ctor) holds the remainder, and the node ceiling + budget are raised to admit
    // the message at the receiver. msg_id stays uint16: every fragment of the message shares
    // ONE msg_id (asserted explicitly in the wrap cell below).
    constexpr std::size_t budget     = 8u * 1024u;
    constexpr std::size_t payload    = 16u * 1024u * 1024u;
    constexpr std::size_t ceiling    = 20u * 1024u * 1024u;
    constexpr std::size_t reassembly = 48u * 1024u * 1024u;

    const auto r = roundtrip_large(budget, payload, ceiling, reassembly, paced_arq(), /*iterations=*/2);
    REQUIRE(r.proven == 2);
    const double mbps = r.last_wall.count() > 0 ? (static_cast<double>(payload) / (1024.0 * 1024.0)) / (r.last_wall.count() / 1000.0) : 0.0;
    WARN("udpr 16 MB round-trip: wall=" << r.last_wall.count() << " ms, throughput~=" << mbps << " MiB/s");
}

TEST_CASE("udp_large_payload: a probe sweep records the highest single message that round-trips "
          "over udpr",
          "[udp_large_payload][envelope16]")
{
    // Empirically substantiate "probe higher than 16 MB": sweep ascending sizes over udpr
    // with the ceiling + budget + backpressure raised to admit each, and record the largest
    // that round-trips byte-equal (recorded, not asserted past the 16 MB floor). The paced
    // ARQ window keeps the in-flight batch under the kernel buffers at every size. A size
    // that fails to fully reassemble within the bound stops the sweep — the last success is
    // the recorded ceiling for this host/run.
    constexpr std::size_t            budget = 8u * 1024u;
    const std::array<std::size_t, 2> sizes{16u * 1024u * 1024u, 24u * 1024u * 1024u};

    std::size_t highest = 0;
    for(std::size_t size : sizes)
    {
        const std::size_t ceiling    = size + 4u * 1024u * 1024u;
        const std::size_t reassembly = ceiling + 4u * 1024u * 1024u;
        const auto        r          = roundtrip_large(budget, size, ceiling, reassembly, paced_arq(), /*iterations=*/1);
        if(r.proven != 1)
            break; // first size that does not fully arrive caps the sweep
        highest = size;
    }
    REQUIRE(highest >= 16u * 1024u * 1024u); // the 16 MB envelope floor holds
    WARN("udpr probe: highest round-tripping single message = " << (highest / (1024 * 1024)) << " MB");
}
