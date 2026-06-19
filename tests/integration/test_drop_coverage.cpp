// over-limit: one cohesive drop-observability matrix; every datagram-path drop cause is forced
// against the one shared engine/reassembler/observer harness, so splitting the per-cause cells
// scatters that shared fixture The drop-observability coverage oracle: every datagram-path drop
// cause is FORCED on a real component, the engine's POSTED drop-observer hook delivers it, and the
// matching per-cause occupancy counter is asserted to have moved. Coverage is proven by forcing
// each cause, not by inspection: a drop site that silently failed to emit would leave its
// cause unobserved and fail the run.
//
// The hook fans out POSTED on the engine's executor over a snapshot — never inline from a
// per-packet drop site (the DoS-amplifier guard). So every leg installs the
// engine's drop_sink onto the component under test, forces the cause, then DRAINS the
// engine executor before asserting the observed event (the posted-delivery contract). The
// receive-side causes ride at band 0 with transport=remote.
//
// Causes forced here: replay / too_old / tamper via crafted datagrams into a
// datagram_authenticated_channel; malformed / reassembly_cap / reassembly_evicted via the
// bounded reassembler (the timeout leg crosses the per-message timer); arq_shed via a
// reliable udp_channel whose congestion=drop sheds a window-full frame; demux_refused via a
// udp_transport whose per-peer cap is zero so the first accept is refused. The asio legs run
// on a real io_context while the sink posts onto the engine's executor; both are drained.
// Each leg loops so a single-run fluke cannot pass.

#include "plexus/crypto/datagram_authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/observer.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/priority.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/reassembler.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/detail/udp_handshake_frame.h"

#include "plexus/topic_qos.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace cause = plexus::io::detail;

namespace {

// The virtual clock the engine's posted fan-out and the reassembler timer fire from —
// the same manual_clock the routing oracle uses.
struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept { return current; }
    static void              reset() noexcept { current = time_point{}; }
    static void              advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type     = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = plexus::inproc::inproc_transport<manual_clock>;
using engine_t    = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;
using test_reassembler =
        plexus::io::detail::reassembler<plexus::inproc::inproc_executor<manual_clock> &,
                                        plexus::inproc::inproc_timer<manual_clock>>;

// An observer that records the cause of every posted drop it sees.
struct recording_drop_observer final : plexus::io::observer
{
    std::vector<cause::drop_cause> seen;
    void on_drop(const cause::drop_event &ev) override { seen.push_back(ev.cause); }

    [[nodiscard]] std::size_t count(cause::drop_cause c) const
    {
        std::size_t n = 0;
        for(auto s : seen)
            if(s == c)
                ++n;
        return n;
    }
};

plexus::io::handshake_fsm_config make_cfg(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return plexus::io::handshake_fsm_config{.self_id                  = id,
                                            .version_major            = 1,
                                            .version_minor            = 0,
                                            .compatible_version_major = 1,
                                            .compatible_version_minor = 0};
}

// The engine exists only as the posted drop-observer registry + executor here — it dials
// nothing. Member ORDER: bus/executor/transport BEFORE the engine.
struct drop_fixture
{
    plexus::inproc::inproc_bus<manual_clock>      bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t                                   transport{ex, bus};
    engine_t engine{transport,
                    ex,
                    make_cfg(0xA1),
                    std::chrono::hours(1),
                    plexus::io::reconnect_config{std::chrono::milliseconds(100),
                                                 std::chrono::milliseconds(10000), std::nullopt,
                                                 std::nullopt},
                    0xC0FFEEu,
                    false};
    recording_drop_observer observer;

    drop_fixture()
    {
        manual_clock::reset();
        engine.add_observer(observer);
    }

    [[nodiscard]] auto sink() { return engine.drop_sink(); }
    void               drain() { ex.drain(); }
};

// --- the crypto datagram-channel scaffold (replay / too_old / tamper) ---

class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void                               close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb) { (void)cb; }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb) { (void)cb; }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        (void)cb;
    }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_sink;
    std::vector<std::byte>                                               m_last;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
};

plexus::crypto::derived_keys fixed_keys()
{
    std::vector<std::byte> psk;
    for(char c : std::string{"a-shared-pre-shared-key-of-decent-length"})
        psk.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>(0x10 + i);
        rs_nonce[i] = static_cast<std::byte>(0xa0 + i);
    }
    for(std::size_t i = 0; i < 32; ++i)
        transcript[i] = static_cast<std::byte>(0x40 + i);
    plexus::crypto::derived_keys k{};
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript, k));
    return k;
}

plexus::crypto::derived_keys swapped(const plexus::crypto::derived_keys &k)
{
    return plexus::crypto::derived_keys{.k_send = k.k_recv, .k_recv = k.k_send};
}

std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type         = plexus::wire::msg_type::unidirectional;
    hdr.session_id   = session_id;
    hdr.timestamp_ns = 7777;
    hdr.payload_len  = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

std::vector<std::vector<std::byte>> seal_datagrams(const plexus::crypto::derived_keys &keys,
                                                   std::size_t                         count)
{
    wire_lower                                                 send_wire;
    plexus::crypto::datagram_authenticated_channel<wire_lower> sender(
            send_wire, plexus::crypto::aead_cipher_id::chacha20_poly1305, keys);
    std::vector<std::vector<std::byte>> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b)
    { on_wire.emplace_back(b.begin(), b.end()); };
    for(std::size_t i = 0; i < count; ++i)
        sender.send(make_frame(7, "dg-" + std::to_string(i)));
    return on_wire;
}

std::vector<std::byte> filler(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 17u + 3u) & 0xFFu);
    return v;
}

// A channel whose reported occupancy is held above the low-water mark so the egress
// scheduler bands every publish instead of draining: a flood past the band depth then
// sheds under the topic's congestion policy (the egress shed the observer must see). It
// records nothing — the shed never reaches send().
struct stall_executor
{
};

struct stall_channel
{
    explicit stall_channel(std::size_t &reported)
            : m_reported(&reported)
    {
    }
    stall_channel(stall_executor &) {}
    stall_channel(stall_executor &, std::error_code &) {}

    void                               send(std::span<const std::byte>) {}
    void                               close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"tcp", "127.0.0.1:0"}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    [[nodiscard]] std::size_t backpressured() const noexcept { return *m_reported; }

    std::size_t *m_reported{nullptr};
};

struct stall_timer
{
    explicit stall_timer(stall_executor &) {}
    stall_timer(stall_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct stall_policy
{
    using executor_type     = stall_executor &;
    using byte_channel_type = stall_channel;
    using timer_type        = stall_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<stall_policy>);

// The same stalling channel reporting the "inproc" scheme: the forwarder classifies its
// subscriber's delivery tier from the channel scheme, so an inproc subscriber whose egress
// band saturates sheds with transport=locality::process — the same egress trio the stream
// tier emits, proving the inproc tier is not a dark drop tier (the egress scheduler governs
// the band ABOVE the channel, transport-agnostically).
struct inproc_stall_channel
{
    explicit inproc_stall_channel(std::size_t &reported)
            : m_reported(&reported)
    {
    }
    inproc_stall_channel(stall_executor &) {}
    inproc_stall_channel(stall_executor &, std::error_code &) {}

    void                               send(std::span<const std::byte>) {}
    void                               close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"inproc", "node-x"}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    [[nodiscard]] std::size_t backpressured() const noexcept { return *m_reported; }

    std::size_t *m_reported{nullptr};
};

struct inproc_stall_policy
{
    using executor_type     = stall_executor &;
    using byte_channel_type = inproc_stall_channel;
    using timer_type        = stall_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<inproc_stall_policy>);

}

TEST_CASE("integration.drop_coverage replay / too_old / tamper post through the engine hook with "
          "their counters",
          "[integration][drop_coverage][datagram]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        drop_fixture fx;
        const auto   keys = fixed_keys();

        // The window width: deliver a sequence this far ahead, then a low one falls below
        // the floor as too_old rather than replay.
        const std::size_t batch = plexus::crypto::k_anti_replay_window_bits + 4;
        const auto        wire  = seal_datagrams(keys, batch);

        wire_lower                                                 recv_wire;
        plexus::crypto::datagram_authenticated_channel<wire_lower> receiver(
                recv_wire, plexus::crypto::aead_cipher_id::chacha20_poly1305, swapped(keys));
        receiver.on_drop(fx.sink());

        // replay: deliver seq 1 twice — the second is a replay within the window.
        recv_wire.feed(std::span<const std::byte>{wire[1]});
        recv_wire.feed(std::span<const std::byte>{wire[1]});

        // tamper: a bad-tag datagram (corrupt the final tag byte).
        auto bad = wire[2];
        bad.back() ^= std::byte{0xff};
        recv_wire.feed(std::span<const std::byte>{bad});

        // too_old: advance the window past the width with the last sealed seq, then replay
        // seq 0 — now below the floor, rejected as reject_old.
        recv_wire.feed(std::span<const std::byte>{wire[batch - 1]});
        recv_wire.feed(std::span<const std::byte>{wire[0]});

        fx.drain();

        REQUIRE(fx.observer.count(cause::drop_cause::replay) == 1);
        REQUIRE(fx.observer.count(cause::drop_cause::tamper) == 1);
        REQUIRE(fx.observer.count(cause::drop_cause::too_old) == 1);
        REQUIRE(receiver.replay_count() == 2); // replay + too_old share the replay counter
        REQUIRE(receiver.tamper_dropped_count() == 1);
    }
}

TEST_CASE("integration.drop_coverage a too-small datagram send surfaces a local malformed drop "
          "instead of silent loss",
          "[integration][drop_coverage][datagram]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        const auto                                                 keys = fixed_keys();
        wire_lower                                                 send_wire;
        plexus::crypto::datagram_authenticated_channel<wire_lower> sender(
                send_wire, plexus::crypto::aead_cipher_id::chacha20_poly1305, keys);

        std::vector<cause::drop_event> drops;
        std::size_t                    on_wire = 0;
        send_wire.m_sink                       = [&](std::span<const std::byte>) { ++on_wire; };
        sender.on_drop([&](const cause::drop_event &ev) { drops.push_back(ev); });

        // A frame shorter than the AEAD-AAD wire header cannot be sealed. Pre-fix send()
        // returned silently; now it emits a malformed drop at the local tier and nothing
        // crosses the wire.
        sender.send(filler(plexus::wire::header_size - 1));

        REQUIRE(on_wire == 0);
        REQUIRE(drops.size() == 1);
        REQUIRE(drops[0].cause == cause::drop_cause::malformed);
        REQUIRE(drops[0].transport == plexus::io::locality::local);

        // A well-formed frame (header-sized or larger) still seals and crosses unaffected.
        sender.send(make_frame(7, "ok"));
        REQUIRE(on_wire == 1);
        REQUIRE(drops.size() == 1);
    }
}

TEST_CASE("integration.drop_coverage malformed / reassembly_cap / reassembly_evicted post through "
          "the engine hook",
          "[integration][drop_coverage][reassembler]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        drop_fixture          fx;
        constexpr std::size_t frag = 256;
        // The cap counts payload AND each entry's slot/present metadata, so size the budget
        // for exactly two 2-fragment partials' payload plus their structural overhead.
        constexpr std::size_t overhead = 2 * sizeof(std::vector<std::byte>) + (2u + 7u) / 8u;
        constexpr std::size_t capbytes =
                2 * (frag + overhead); // room for two single-fragment partials
        test_reassembler r{fx.ex,
                           {.total_memory_cap    = capbytes,
                            .per_message_timeout = std::chrono::milliseconds(1000)}};
        r.on_drop(fx.sink());

        // malformed: frag_idx >= frag_cnt.
        REQUIRE(r.feed(1, 3, 2, filler(frag)) == test_reassembler::outcome::dropped_malformed);

        // reassembly_cap: open two partials filling the cap, then a third NEW partial is
        // refused (its bytes would breach the total-memory cap).
        REQUIRE(r.feed(10, 0, 2, filler(frag)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(11, 0, 2, filler(frag)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(12, 0, 2, filler(frag)) == test_reassembler::outcome::dropped_cap);

        // reassembly_evicted: a stalled partial reclaimed when the per-message timer fires.
        const std::size_t evicted_before = fx.observer.count(cause::drop_cause::reassembly_evicted);
        manual_clock::advance(std::chrono::milliseconds(1001));
        fx.drain();

        REQUIRE(fx.observer.count(cause::drop_cause::malformed) == 1);
        REQUIRE(fx.observer.count(cause::drop_cause::reassembly_cap) == 1);
        REQUIRE(fx.observer.count(cause::drop_cause::reassembly_evicted) > evicted_before);
    }
}

TEST_CASE("integration.drop_coverage arq_shed posts through the engine hook when a reliable "
          "window-full frame is shed",
          "[integration][drop_coverage][udp]")
{
    using plexus::asio::udp_channel;

    for(int loop = 0; loop < 4; ++loop)
    {
        drop_fixture              fx;
        ::asio::io_context        io;
        ::asio::ip::udp::endpoint dest(::asio::ip::make_address_v4("127.0.0.1"), 9);
        plexus::asio::udp_server  server(io);

        // A reliable_datagram channel with congestion=drop_newest: once the ARQ window is
        // full, each further reliable send is SHED at the publisher (arq_shed) rather than
        // queued. No peer ever acks, so the window never reopens.
        udp_channel ch(io, server, dest, udp_channel::default_max_payload, {},
                       plexus::io::congestion::drop_newest, udp_channel::default_backpressure_bytes,
                       plexus::io::detail::udp_channel_mode::reliable_datagram);
        ch.on_drop(fx.sink());

        const auto  payload = filler(64);
        std::size_t shed    = 0;
        for(int i = 0; i < 4096; ++i)
            if(ch.send_reliable(payload) == udp_channel::submit_result::window_full)
                ++shed;

        io.poll();
        fx.drain();

        REQUIRE(shed > 0);
        REQUIRE(ch.dropped_count() == shed);
        REQUIRE(fx.observer.count(cause::drop_cause::arq_shed) == shed);
    }
}

TEST_CASE("integration.drop_coverage demux_refused posts through the engine hook when the per-peer "
          "cap rejects an accept",
          "[integration][drop_coverage][udp]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        drop_fixture       fx;
        ::asio::io_context io;

        // A transport whose per-peer demux cap is ZERO: the first handshake-request accept
        // mints a channel but the demux insert is refused → demux_refused.
        plexus::asio::udp_transport acceptor(io, plexus::asio::udp_channel::default_max_payload,
                                             plexus::asio::udp_transport::arq_type::default_ladder,
                                             {}, plexus::io::congestion::block, 0);
        acceptor.on_drop(fx.sink());
        acceptor.listen({"udp", "127.0.0.1:0"});

        const std::uint16_t port = acceptor.port();
        REQUIRE(port != 0);

        // Craft a best_effort handshake-request datagram (encode_handshake_into already
        // wraps the UDP envelope) and send it from a client socket.
        std::vector<std::byte> datagram;
        plexus::io::detail::encode_handshake_into(
                datagram, plexus::io::detail::udp_hs_type::request,
                plexus::io::detail::udp_channel_mode::best_effort);

        ::asio::ip::udp::socket   client(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        ::asio::ip::udp::endpoint to(::asio::ip::make_address_v4("127.0.0.1"), port);

        for(int i = 0; i < 200 && fx.observer.count(cause::drop_cause::demux_refused) == 0; ++i)
        {
            client.send_to(::asio::buffer(datagram.data(), datagram.size()), to);
            io.run_for(std::chrono::milliseconds(5));
            fx.drain();
        }

        REQUIRE(fx.observer.count(cause::drop_cause::demux_refused) >= 1);
    }
}

TEST_CASE("integration.drop_coverage an egress shed bumps the per-band counter AND must reach an "
          "installed observer",
          "[integration][drop_coverage][egress]")
{
    using forwarder = plexus::io::message_forwarder<stall_policy>;

    for(int loop = 0; loop < 4; ++loop)
    {
        std::size_t             reported = 0;
        stall_channel           ch{reported};
        forwarder               fwd{};
        recording_drop_observer observer;

        // A bounded band under congestion=drop_newest: stall the destination so every
        // publish bands, then flood past the band depth so the surplus sheds.
        fwd.on_drop([&observer](const cause::drop_event &ev) { observer.on_drop(ev); });
        fwd.declare("shed", plexus::topic_qos{.congestion = plexus::io::congestion::drop_newest});
        REQUIRE(fwd.attach_for_fanout(forwarder::peer{ch, "node-a"}, "shed"));

        reported           = plexus::io::detail::k_low_water + 1;
        const int  flood   = static_cast<int>(plexus::io::detail::k_band_depth) + 16;
        const auto payload = filler(64);
        for(int i = 0; i < flood; ++i)
            fwd.publish("shed", std::span<const std::byte>{payload});

        // The always-on per-band counter moved: the surplus past the depth was shed as
        // drop_newest. This counter semantics is the pre-existing truth and stays additive.
        const std::size_t band = plexus::io::detail::band_of(plexus::io::priority::normal);
        const std::size_t shed = fwd.dropped("shed", band, cause::drop_cause::drop_newest);
        REQUIRE(shed > 0);

        // The egress shed bumps the always-on counter AND routes through the forwarder's
        // on_drop seam to the installed observer: the observer sees every shed.
        REQUIRE(observer.count(cause::drop_cause::drop_newest) >= shed);
    }
}

TEST_CASE("integration.drop_coverage an inproc subscriber's egress shed reaches the observer with "
          "transport=process",
          "[integration][drop_coverage][inproc]")
{
    using forwarder = plexus::io::message_forwarder<inproc_stall_policy>;

    // The inproc tier is not a dark drop tier: the egress scheduler bands an inproc
    // subscriber ABOVE the channel and sheds transport-agnostically, so the drop reaches
    // the observer carrying transport=locality::process (the inproc scheme's tier). Loop so
    // a single-run fluke cannot pass.
    for(int loop = 0; loop < 4; ++loop)
    {
        std::size_t                    reported = 0;
        inproc_stall_channel           ch{reported};
        forwarder                      fwd{};
        std::vector<cause::drop_event> drops;

        fwd.on_drop([&drops](const cause::drop_event &ev) { drops.push_back(ev); });
        fwd.declare("inproc-shed",
                    plexus::topic_qos{.congestion = plexus::io::congestion::drop_newest});
        REQUIRE(fwd.attach_for_fanout(forwarder::peer{ch, "node-x"}, "inproc-shed"));

        reported           = plexus::io::detail::k_low_water + 1;
        const int  flood   = static_cast<int>(plexus::io::detail::k_band_depth) + 16;
        const auto payload = filler(64);
        for(int i = 0; i < flood; ++i)
            fwd.publish("inproc-shed", std::span<const std::byte>{payload});

        std::size_t shed_to_observer = 0;
        for(const auto &ev : drops)
            if(ev.cause == cause::drop_cause::drop_newest)
            {
                REQUIRE(ev.transport == plexus::io::locality::process);
                ++shed_to_observer;
            }
        REQUIRE(shed_to_observer > 0);
    }
}
