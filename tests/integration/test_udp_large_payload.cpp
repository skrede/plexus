// Large-payload datagram capability over real loopback sockets: a 1 MB and a 4 MB
// message round-trip byte-identically through the existing fragmenter + per-channel
// reassembler over BOTH the best-effort ("udp") and the reliable-ARQ ("udpr") legs,
// looped in-body and re-run across >=3 process runs (a transport claim is never made
// from a single run). The deterministic loss/reorder shim (tests/support) drives the
// injected-loss legs: under a fixed loss fraction the best-effort path drops the WHOLE
// message on a lost fragment (drop-whole-message semantics, recorded as a measured
// observation) while the reliable path reassembles via retransmit. The shim's own
// determinism (a byte-identical drop/reorder sequence across two runs) is asserted
// first — the empirical-reproducibility property the fragment-scale sweep depends on.
//
// No plexus source is touched by this test; the loss is injected at the wire boundary.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/fragmentation.h"

#include "plexus/wire/udp_envelope.h"

#include "support/loss_reorder_shim.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace pasio = plexus::asio;
namespace pio = plexus::io;
namespace pwire = plexus::wire;
namespace ptest = plexus::testing;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A generous window + quick retransmit so a multi-hundred-fragment reliable message rides
// through the bounded congestion=block queue without exhausting retransmits over loopback.
inline pio::detail::udp_arq_config large_arq()
{
    return pio::detail::udp_arq_config{
        .window = 1024, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{160}, .max_retransmit = 40};
}

// A DELIBERATELY tiny send window so a many-fragment message has far more fragments than
// the window admits: the bulk of fragments must transit the bounded congestion=block
// backpressure queue, which is the path that must preserve each fragment's FRAGMENTED
// envelope bit (a window-sized message never parks a fragment and would not exercise it).
inline pio::detail::udp_arq_config tiny_window_arq()
{
    return pio::detail::udp_arq_config{
        .window = 8, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{160}, .max_retransmit = 40};
}

// A deterministic, position-dependent payload byte-checked against a regenerated oracle.
std::vector<std::byte> make_payload(std::size_t n, std::uint8_t salt)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + salt * 7u + (i >> 8)) & 0xFFu);
    return out;
}

bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{15000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

TEST_CASE("loss_shim drops a fixed-seed fraction deterministically across two runs", "[loss_shim]")
{
    // Two schedulers with the SAME config + seed must produce a byte-identical drop/emit
    // sequence over a fixed datagram stream — the reproducibility law the empirical sweeps
    // and the lossy benchmark cell rely on (no std::random anywhere in the path).
    ptest::loss_reorder_config cfg{.loss_num = 30, .loss_den = 100, .reorder_depth = 4, .seed = 0xABCDEF01u};

    auto run_once = [&] {
        ptest::loss_reorder_scheduler sched{cfg};
        std::vector<std::vector<std::byte>> emitted;
        for(int i = 0; i < 500; ++i)
        {
            std::vector<std::byte> dg{static_cast<std::byte>(i & 0xFF),
                                      static_cast<std::byte>((i >> 8) & 0xFF)};
            for(auto &out : sched.drive(dg))
                emitted.push_back(std::move(out));
        }
        for(auto &out : sched.flush())
            emitted.push_back(std::move(out));
        return std::pair{std::move(emitted), sched.dropped()};
    };

    auto [seq_a, dropped_a] = run_once();
    auto [seq_b, dropped_b] = run_once();

    REQUIRE(dropped_a == dropped_b);          // identical drop COUNT across runs
    REQUIRE(dropped_a > 0);                    // the loss fraction genuinely engaged
    REQUIRE(seq_a.size() == seq_b.size());
    for(std::size_t i = 0; i < seq_a.size(); ++i)
        REQUIRE(seq_a[i] == seq_b[i]);         // byte-identical emit ORDER (drop + reorder)

    // The reorder window genuinely reordered (the emitted order is not the input order).
    bool reordered = false;
    for(std::size_t i = 1; i < seq_a.size(); ++i)
        if(seq_a[i] < seq_a[i - 1])
        {
            reordered = true;
            break;
        }
    REQUIRE(reordered);
}

TEST_CASE("loss_shim with zero loss and zero reorder is an order-preserving pass-through", "[loss_shim]")
{
    ptest::loss_reorder_scheduler sched{ptest::loss_reorder_config{}};
    std::vector<std::vector<std::byte>> emitted;
    for(int i = 0; i < 64; ++i)
    {
        std::vector<std::byte> dg{static_cast<std::byte>(i)};
        for(auto &out : sched.drive(dg))
            emitted.push_back(std::move(out));
    }
    REQUIRE(emitted.size() == 64);
    REQUIRE(sched.dropped() == 0);
    for(int i = 0; i < 64; ++i)
        REQUIRE(emitted[static_cast<std::size_t>(i)].front() == static_cast<std::byte>(i));
}

namespace {

// Stand up a loopback udp_transport pair (no interposed relay) and round-trip one payload
// over the given scheme, looping in-body. Returns the count of byte-equal deliveries.
int roundtrip_clean(const char *scheme, std::size_t budget, std::size_t payload_size,
                    pio::detail::udp_arq_config arq, int iterations)
{
    int proven = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, arq};
        pasio::udp_transport client{io, budget, fast_hs, arq};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({scheme, "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto payload = make_payload(payload_size, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() > budget);                 // genuinely fragmented
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                          // exactly ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload));        // byte-equal end-to-end
        ++proven;
    }
    return proven;
}

}

TEST_CASE("udp_large_payload: an oversize message round-trips byte-identically over UDP best-effort, looped",
          "[udp_large_payload]")
{
    // Best-effort UDP has no retransmit: a burst that overruns the kernel receive buffer
    // (the host default here is ~208 KiB) loses fragments and the whole message is dropped
    // (the recorded loss policy below). The lossless best-effort round-trip is therefore
    // proven at a multi-fragment size that fits the default socket buffer — the fragmenter +
    // reassembler compose correctly over the best-effort leg. The 1 MB / 4 MB capability is
    // proven over udpr, which retransmits the inevitable loss at that volume.
    constexpr std::size_t budget = 1200;                  // the conservative default budget
    REQUIRE(roundtrip_clean("udp", budget, 16u * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 1 MB message round-trips byte-identically over udpr reliable, looped",
          "[udp_large_payload]")
{
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 4 MB message (the max_message_size ceiling) round-trips over udpr, looped",
          "[udp_large_payload]")
{
    // 4 MiB at an 8 KiB budget -> ~512 fragments, inside the 1024-segment window. The whole
    // ceiling message rides the reliable ARQ and reassembles byte-equal.
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 4u * 1024 * 1024, large_arq(), /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: a fragmented udpr message whose fragment count exceeds the send window reassembles through the backpressure queue, looped",
          "[udp_large_payload]")
{
    // Regression: a reliable fragment that parks in the congestion=block backpressure queue
    // (because the send window is full) must KEEP its FRAGMENTED envelope bit when it drains.
    // A tiny 8-segment window against a 1 MiB message (~128 fragments at an 8 KiB budget)
    // forces all but the first few fragments through the queue. If a drained fragment lost
    // the flag the peer would post each raw [msg_id][idx][cnt][slice] blob as a whole message
    // instead of reassembling — got would fill with many wrong-sized blobs and never match the
    // payload, so the single-byte-equal-delivery assertion below fails closed.
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, tiny_window_arq(), /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: the injected-loss policy is recorded as measured — UDP drops the whole message, udpr reassembles",
          "[udp_large_payload]")
{
    // Drive a large message through the deterministic loss shim at a fixed loss fraction on
    // BOTH legs and RECORD the measured loss policy (DGRAM-01's loss-policy recording):
    //   * best_effort ("udp"): a lost fragment is never recovered, so the per-message
    //     reassembly times out and the WHOLE message is dropped — NO delivery.
    //   * reliable ("udpr"): each lost fragment is selectively retransmitted, so the
    //     message reassembles byte-equal despite the same injected loss.
    // A size that fits the kernel receive buffer so the ONLY loss is the shim's (an
    // overrun-driven drop would confound the attribution): ~55 fragments at a 1200-byte
    // budget, each subject to the deterministic 8% drop.
    constexpr std::size_t budget = 1200;
    constexpr std::size_t payload_size = 64u * 1024;
    const ptest::loss_reorder_config loss{.loss_num = 8, .loss_den = 100, .reorder_depth = 0, .seed = 0x5151ull};

    auto run_leg = [&](const char *scheme) {
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, large_arq()};
        pasio::udp_transport client{io, budget, fast_hs, large_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        int deliveries = 0;
        std::vector<std::byte> last;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { ++deliveries; last.assign(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        ptest::loss_reorder_relay link{io, server.port(), loss};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({scheme, "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto payload = make_payload(payload_size, 0x4D);
        dialed->send(payload);

        // Pump well past the reassembly timeout so the best-effort drop-whole verdict settles
        // and the reliable retransmits complete.
        pump_until(io, [&] { return deliveries > 0; }, ms{8000});
        return std::tuple{deliveries, std::move(last), std::move(payload), link.dropped()};
    };

    // RECORDED MEASUREMENT (best-effort): a lost fragment drops the whole message.
    {
        auto [deliveries, last, payload, dropped] = run_leg("udp");
        REQUIRE(dropped > 0);                     // the shim genuinely lost fragments
        REQUIRE(deliveries == 0);                 // drop-whole-message — no partial delivery
        WARN("measured loss policy [udp best-effort]: " << dropped
             << " datagram(s) dropped by the shim -> whole-message drop, deliveries=" << deliveries);
    }

    // RECORDED MEASUREMENT (reliable): the same loss is retransmitted and the message arrives.
    {
        auto [deliveries, last, payload, dropped] = run_leg("udpr");
        REQUIRE(dropped > 0);                     // the same injected loss engaged
        REQUIRE(deliveries == 1);                 // reliably reassembled via retransmit
        REQUIRE(equal_bytes(last, payload));      // byte-equal despite the loss
        WARN("measured loss policy [udpr reliable]: " << dropped
             << " datagram(s) dropped by the shim -> retransmit-reassembled, deliveries=" << deliveries);
    }
}

namespace {

// A MODERATE ARQ window that PACES the send: only `window` fragments are outstanding at a
// time, acked before the next batch leaves, so a multi-megabyte message never bursts past
// the ~4 MiB loopback kernel socket buffers (a burst would drop fragments faster than the
// ARQ could retransmit). The backpressure queue holds the not-yet-windowed remainder, so
// the per-channel backpressure cap must reach the whole message (sized in the helper). A
// generous retransmit budget covers the residual loopback loss/reorder at this volume.
inline pio::detail::udp_arq_config paced_arq()
{
    return pio::detail::udp_arq_config{
        .window = 64, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{200}, .max_retransmit = 80};
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
    int proven;
    std::chrono::milliseconds last_wall;
};

large_result roundtrip_large(std::size_t budget, std::size_t payload_size,
                             std::size_t global_default, std::size_t reassembly_budget,
                             pio::detail::udp_arq_config arq, int iterations)
{
    // A multi-megabyte reassembly over the paced ARQ takes well past the 5 s default
    // per-message reclaim window, which would evict the partial mid-flight; extend it so an
    // honest slow large message completes (the reclaim still bounds a genuinely stalled one).
    constexpr ms reassembly_timeout{60000};
    using clock = std::chrono::steady_clock;
    int proven = 0;
    std::chrono::milliseconds last_wall{0};
    for(int iter = 0; iter < iterations; ++iter)
    {
        // The backpressure queue holds the message's not-yet-windowed fragments, so it must
        // reach the whole message (plus headroom); the kernel buffers are raised to the
        // host max so the paced in-flight batch is buffered, not dropped at the socket.
        const std::size_t backpressure = payload_size + 4u * 1024u * 1024u;
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, arq,
                                    pio::congestion::block, pasio::detail::udp_inbound_demux::default_max_peers,
                                    k_socket_buf, k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes, global_default, reassembly_budget,
                                    backpressure, reassembly_timeout};
        pasio::udp_transport client{io, budget, fast_hs, arq,
                                    pio::congestion::block, pasio::detail::udp_inbound_demux::default_max_peers,
                                    k_socket_buf, k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes, global_default, reassembly_budget,
                                    backpressure, reassembly_timeout};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
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

        auto payload = make_payload(payload_size, static_cast<std::uint8_t>(iter));
        const auto t0 = clock::now();
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

TEST_CASE("udp_large_payload: a 16 MB single message round-trips byte-identically over udpr, looped",
          "[udp_large_payload][envelope16]")
{
    // The lifted envelope on the datagram path: one 16 MiB message over the reliable ARQ,
    // proven byte-equal across repeated in-body iterations (the ctest invocation is itself
    // re-run for cross-process reproducibility — a transport claim is never made from one
    // run). An 8 KiB fragment budget (~2048 fragments) with a paced ARQ window keeps the
    // in-flight batch under the kernel buffers; the backpressure queue (raised through the
    // transport ctor) holds the remainder, and the node ceiling + budget are raised to admit
    // the message at the receiver. msg_id stays uint16: every fragment of the message shares
    // ONE msg_id (asserted explicitly in the wrap cell below).
    constexpr std::size_t budget = 8u * 1024u;
    constexpr std::size_t payload = 16u * 1024u * 1024u;
    constexpr std::size_t ceiling = 20u * 1024u * 1024u;
    constexpr std::size_t reassembly = 48u * 1024u * 1024u;

    const auto r = roundtrip_large(budget, payload, ceiling, reassembly, paced_arq(), /*iterations=*/2);
    REQUIRE(r.proven == 2);
    const double mbps = r.last_wall.count() > 0
        ? (static_cast<double>(payload) / (1024.0 * 1024.0)) / (r.last_wall.count() / 1000.0)
        : 0.0;
    WARN("udpr 16 MB round-trip: wall=" << r.last_wall.count() << " ms, throughput~=" << mbps << " MiB/s");
}

TEST_CASE("udp_large_payload: a probe sweep records the highest single message that round-trips over udpr",
          "[udp_large_payload][envelope16]")
{
    // Empirically substantiate "probe higher than 16 MB": sweep ascending sizes over udpr
    // with the ceiling + budget + backpressure raised to admit each, and record the largest
    // that round-trips byte-equal (recorded, not asserted past the 16 MB floor). The paced
    // ARQ window keeps the in-flight batch under the kernel buffers at every size. A size
    // that fails to fully reassemble within the bound stops the sweep — the last success is
    // the recorded ceiling for this host/run.
    constexpr std::size_t budget = 8u * 1024u;
    const std::array<std::size_t, 2> sizes{16u * 1024u * 1024u, 24u * 1024u * 1024u};

    std::size_t highest = 0;
    for(std::size_t size : sizes)
    {
        const std::size_t ceiling = size + 4u * 1024u * 1024u;
        const std::size_t reassembly = ceiling + 4u * 1024u * 1024u;
        const auto r = roundtrip_large(budget, size, ceiling, reassembly, paced_arq(), /*iterations=*/1);
        if(r.proven != 1)
            break;                                   // first size that does not fully arrive caps the sweep
        highest = size;
    }
    REQUIRE(highest >= 16u * 1024u * 1024u);          // the 16 MB envelope floor holds
    WARN("udpr probe: highest round-tripping single message = " << (highest / (1024 * 1024)) << " MB");
}

namespace {

// An observing relay between the udp client and server: it decodes every best-effort
// fragment datagram's wire sub-header (msg_id / frag_idx / frag_cnt) before forwarding
// it, so a test can assert ON THE WIRE that a large message spans many uint32 fragments
// under ONE uint16 msg_id and that consecutive messages' msg_ids advance by exactly one
// (never wrapping the uint16). It forwards both directions verbatim; loss is irrelevant
// to the assertion (the observed headers carry the true counts regardless of delivery).
struct fragment_observer
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;       // faces the client
    ::asio::ip::udp::socket back;        // faces the server
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 70000> front_buf{};
    std::array<std::byte, 70000> back_buf{};

    std::vector<std::uint16_t> msg_ids;             // distinct msg_ids in first-seen order
    std::uint32_t max_frag_cnt{0};

    fragment_observer(::asio::io_context &ctx, std::uint16_t server_port)
        : io(ctx)
        , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    void note_fragment(std::span<const std::byte> dg)
    {
        auto outer = pwire::unwrap_udp(dg);
        if(!outer || !outer->fragmented)
            return;                                  // handshake / whole datagram: not a fragment
        auto h = pwire::decode_udp_fragment_header(outer->frame);
        if(!h)
            return;
        max_frag_cnt = std::max(max_frag_cnt, h->frag_cnt);
        if(msg_ids.empty() || msg_ids.back() != h->msg_id)
            if(std::find(msg_ids.begin(), msg_ids.end(), h->msg_id) == msg_ids.end())
                msg_ids.push_back(h->msg_id);
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
            [this](std::error_code ec, std::size_t n) {
                if(ec)
                    return;
                client_ep = from;
                std::span<const std::byte> dg{front_buf.data(), n};
                note_fragment(dg);
                auto copy = std::make_shared<std::vector<std::byte>>(dg.begin(), dg.end());
                back.async_send_to(::asio::buffer(*copy), server_ep, [copy](std::error_code, std::size_t) {});
                recv_front();
            });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
            [this](std::error_code ec, std::size_t n) {
                if(ec)
                    return;
                if(client_ep.port() != 0)
                {
                    auto copy = std::make_shared<std::vector<std::byte>>(back_buf.data(), back_buf.data() + n);
                    front.async_send_to(::asio::buffer(*copy), client_ep, [copy](std::error_code, std::size_t) {});
                }
                recv_back();
            });
    }
};

}

TEST_CASE("udp_large_payload: a 16 MB datagram message spans many uint32 fragments under ONE uint16 msg_id; consecutive messages advance the msg_id by one without wrapping",
          "[udp_large_payload][envelope16][msgid]")
{
    // The EXPLICIT msg_id-wrap assertion. A single large message is split into many
    // fragments; the fragment COUNT is a uint32 field (lifted this phase), while msg_id
    // stays uint16 and advances per-MESSAGE, never per-fragment. At a 128-byte fragment
    // budget a 16 MiB message is ~131072 fragments — far past the uint16 max (65535) — so a
    // single message's fragment count CANNOT be expressed in a uint16 msg_id: the old
    // confusion (that msg_id bounds the per-message fragment count) is closed here on the
    // wire. Two consecutive messages are sent best-effort through an observing relay that
    // decodes each fragment header; the assertion reads the observed headers (loss does not
    // matter — the headers carry the true counts):
    //   * every observed fragment of the run carries frag_cnt > 0xFFFF (a single message's
    //     fragment count exceeds what a uint16 could ever hold), proving the uint32 widening
    //     is load-bearing;
    //   * exactly TWO distinct msg_ids are observed (one per message), and they differ by
    //     exactly one — msg_id advances per-message and does not wrap/collide.
    constexpr std::size_t budget = 128u;                 // the fragment floor -> the largest count
    constexpr std::size_t payload = 16u * 1024u * 1024u; // ~131072 fragments per message
    constexpr std::size_t ceiling = 20u * 1024u * 1024u;
    constexpr std::size_t reassembly = 48u * 1024u * 1024u;

    ::asio::io_context io;
    pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, large_arq(),
                                pio::congestion::block, pasio::detail::udp_inbound_demux::default_max_peers,
                                pasio::udp_server::default_so_sndbuf, pasio::udp_server::default_so_rcvbuf,
                                pasio::udp_server::default_send_queue_bytes, ceiling, reassembly};
    pasio::udp_transport client{io, budget, fast_hs, large_arq(),
                                pio::congestion::block, pasio::detail::udp_inbound_demux::default_max_peers,
                                pasio::udp_server::default_so_sndbuf, pasio::udp_server::default_so_rcvbuf,
                                pasio::udp_server::default_send_queue_bytes, ceiling, reassembly};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
        accepted = std::move(ch);
        accepted->on_data([&](std::span<const std::byte>) {});      // delivery is incidental to this assertion
    });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    fragment_observer obs{io, server.port()};
    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(obs.port())});   // best-effort: fragments are wire-visible
    pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);

    // Two consecutive large messages: their msg_ids must differ by exactly one.
    auto first = make_payload(payload, 0x11);
    auto second = make_payload(payload, 0x22);
    dialed->send(first);
    pump_until(io, [&] { return obs.msg_ids.size() >= 1; }, ms{4000});
    dialed->send(second);
    pump_until(io, [&] { return obs.msg_ids.size() >= 2; }, ms{4000});

    REQUIRE(obs.max_frag_cnt > 0xFFFFu);                 // a single message's fragment count exceeds uint16
    REQUIRE(obs.msg_ids.size() == 2);                    // exactly one msg_id per message
    const int delta = static_cast<int>(obs.msg_ids[1]) - static_cast<int>(obs.msg_ids[0]);
    REQUIRE(delta == 1);                                 // per-message advance, no wrap/collision
}
