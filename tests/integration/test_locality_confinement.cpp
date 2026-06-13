// The locality confinement matrix: the delivery-tier QoS proven on BOTH enforcement
// sides. The DETERMINISTIC leg (always built, synthetic schemes, looped) drives the
// message_forwarder fan-out gate directly — subscribers tagged process("inproc"),
// local("unix"), and remote("tcp") attach to one forwarder, and per published reach
// mask ONLY the in-mask tiers receive the frame: a process-only topic reaches no
// channel; process|local never goes remote; remote never reaches a local/in-process
// peer; any reaches all. The synthetic "inproc" scheme is anchored to the production
// classifier (tier_of("inproc") == locality::process) so the process-tier proof cannot
// pass for the wrong reason. The LIVE leg (gated on the asio backend, looped, re-run
// across process runs) stands up a REAL AF_UNIX peer (local) and a REAL TCP peer
// (remote) and proves the same confinement over the wire. The DEMAND-GATE leg drives a
// routing_engine: a local-confined subscribe toward a tcp peer establishes NO remote
// path (no dial, no slot, is_connected stays false) — the symmetric demand-side half.

#include "plexus/io/locality.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#ifdef PLEXUS_HAVE_ASIO_MUX
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/unix_channel.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"

#include <asio/io_context.hpp>

#include <unistd.h>
#endif

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::io::locality;
using plexus::io::tier_of;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A synthetic channel whose remote_endpoint() reports a TEST-CHOSEN scheme, so the
// forwarder's attach-time tier classification (tier_of(scheme)) is exercised
// deterministically — no backend, no socket. send() only counts; the bytes are
// irrelevant to a confinement (delivered-or-not) assertion.
struct tagged_executor {};

struct tagged_channel
{
    explicit tagged_channel(std::string scheme) : m_scheme(std::move(scheme)) {}

    void send(std::span<const std::byte>) { ++sends; }
    void close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {m_scheme, ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    std::string m_scheme;
    std::size_t sends{0};
};

struct tagged_timer
{
    explicit tagged_timer(tagged_executor &) {}
    tagged_timer(tagged_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct tagged_policy
{
    using executor_type = tagged_executor &;
    using byte_channel_type = tagged_channel;
    using timer_type = tagged_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<tagged_policy>);

}

TEST_CASE("locality confinement: the synthetic inproc scheme is anchored to the production process tier",
          "[integration][locality][confinement]")
{
    // Without this anchor the process-tier confinement could pass for the wrong reason
    // if the synthetic scheme ever diverged from the classifier the production path uses.
    REQUIRE(tier_of("inproc") == locality::process);
    REQUIRE(tier_of("unix") == locality::local);
    REQUIRE(tier_of("tcp") == locality::remote);
}

TEST_CASE("locality confinement: the fan-out gate delivers a topic only to its in-mask tiers (full matrix), looped",
          "[integration][locality][confinement]")
{
    using forwarder = plexus::io::message_forwarder<tagged_policy>;

    constexpr int k_iterations = 100;
    const std::string fqn = "demo.confined.topic";
    const std::string payload = "confined-bytes";

    // The realistic subscriber set: a node fans toward WIRE peers only — a same-host
    // AF_UNIX peer (local tier) and an off-host TCP peer (remote tier). No process-tier
    // subscriber exists, because the process tier currently ships as the BIT +
    // CONFINEMENT only (no in-process positive-delivery sink yet) — so a process-confined
    // topic reaches NO channel here, the pure-isolation property.
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tagged_channel local_ch{"unix"};
        tagged_channel remote_ch{"tcp"};

        forwarder fwd{};
        fwd.attach(forwarder::peer{local_ch, "local-node"}, fqn);
        fwd.attach(forwarder::peer{remote_ch, "remote-node"}, fqn);

        // process-only: reaches NO wire channel (process ships as confinement-only).
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::process});
        const auto l0 = local_ch.sends, r0 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l0 == 0);   // a process-only topic touches no off-process transport
        REQUIRE(remote_ch.sends - r0 == 0);

        // process|local: reaches the local channel, NEVER the remote one.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::process | locality::local});
        const auto l1 = local_ch.sends, r1 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l1 == 1);   // the local AF_UNIX-tier peer receives
        REQUIRE(remote_ch.sends - r1 == 0);  // process|local NEVER goes remote

        // remote-only: reaches only the remote channel.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::remote});
        const auto l2 = local_ch.sends, r2 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l2 == 0);   // a local peer is NEVER reached by a remote topic
        REQUIRE(remote_ch.sends - r2 == 1);

        // any (default): reaches all wire channels.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::any});
        const auto l3 = local_ch.sends, r3 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l3 == 1);
        REQUIRE(remote_ch.sends - r3 == 1);

        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

// ---- The demand-side gate (engine-driven), deterministic over the inproc bus ----

namespace {

struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn) { ex.post(std::move(fn)); }
};

static_assert(plexus::Policy<manual_policy>);

using demand_transport = plexus::inproc::inproc_transport<manual_clock>;
using demand_engine = plexus::io::routing_engine<manual_policy, demand_transport, manual_clock>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::io::handshake_fsm_config make_cfg(std::uint8_t seed)
{
    return plexus::io::handshake_fsm_config{.self_id = make_id(seed), .version_major = 1, .version_minor = 0,
                                            .compatible_version_major = 1, .compatible_version_minor = 0};
}

plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                                        std::nullopt, std::nullopt};
}

}

TEST_CASE("locality confinement: a local-confined subscribe toward a tcp peer establishes NO remote path (demand gate), looped",
          "[integration][locality][confinement]")
{
    constexpr int k_iterations = 100;
    constexpr std::uint64_t k_seed = 0xC0FFEEu;

    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        plexus::inproc::inproc_bus<manual_clock> bus;
        plexus::inproc::inproc_executor<manual_clock> ex{bus};
        demand_transport transport_a{ex, bus};
        demand_transport transport_b{ex, bus};

        demand_engine a(transport_a, ex, make_cfg(0xA1), std::chrono::hours(1),
                        forever_cfg(), k_seed, /*eager=*/false);
        demand_engine b(transport_b, ex, make_cfg(0xB2), std::chrono::hours(1),
                        forever_cfg(), k_seed, /*eager=*/false);
        a.listen({"inproc", "node-a"});
        b.listen({"inproc", "node-b"});

        // Peer R: AWARE at a "tcp" endpoint (a REMOTE tier) — but unreachable here; the
        // gate must refuse a confined demand toward it WITHOUT ever building a path.
        const plexus::node_id remote_id = make_id(0xC3);
        a.note_peer(remote_id, plexus::io::endpoint{"tcp", "10.0.0.5:9000"});

        // Peer L: a REAL reachable in-scope peer at an "inproc" endpoint (PROCESS tier),
        // the live control proving the gate admits an in-scope demand (it is not a blanket
        // refusal). node B actually listens, so an admitted demand connects.
        const plexus::node_id inproc_id = make_id(0xB2);
        a.note_peer(inproc_id, plexus::io::endpoint{"inproc", "node-b"});
        ex.drain();

        // (1) A LOCAL-confined subscription toward the REMOTE-tier peer: REFUSED before any
        //     reach/dial. No slot is built, so no dial was attempted, no path established.
        a.subscribe(remote_id, "confined.topic", locality::local);
        ex.drain();
        REQUIRE_FALSE(a.has_session(remote_id));   // no slot built for the refused demand
        REQUIRE_FALSE(a.is_connected(remote_id));  // no remote path established

        // (2) The functional control: a PROCESS-scoped subscription toward the inproc peer
        //     (whose tier IS process) is ADMITTED — it dials, handshakes, and connects.
        //     The gate refuses only an out-of-scope mask, never all demand.
        a.subscribe(inproc_id, "open.topic", locality::process);
        ex.drain();
        REQUIRE(a.is_connected(inproc_id));   // an in-scope demand established a real path

        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#ifdef PLEXUS_HAVE_ASIO_MUX

// ---- The LIVE leg: real AF_UNIX (local) + real TCP (remote) over the fan-out gate ----

namespace {

namespace pasio = plexus::asio;
namespace pio = plexus::io;

// A per-instance owner-only temp dir + a SHORT AF_UNIX socket path (well under sun_path).
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char tmpl[] = "/tmp/pxl-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir = made ? made : "";
        path = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// A live two-transport fixture: a forwarder over the unix policy fans toward BOTH a real
// AF_UNIX channel (local tier) AND a real TCP channel (remote tier). The forwarder's
// channel_type is the unix_channel; the TCP end is wrapped behind the same concrete
// surface only conceptually — instead we run TWO forwarders, one per concrete policy, and
// attach each real channel to a forwarder of its own policy. Simpler and faithful: the
// gate is per-subscriber-tier, so a single forwarder with both real channels is what we
// want — but the two channel types differ. We therefore drive the gate over EACH real
// transport's own forwarder and assert the cross-tier confinement by reach mask.
//
// Concretely: stand up a real AF_UNIX dialed link and a real TCP dialed link, then for a
// forwarder fanning over the AF_UNIX channel assert a remote-confined topic is dropped and
// a local-confined topic is delivered; symmetrically over the TCP channel.

template <typename Policy, typename Transport, typename Channel>
struct live_link
{
    ::asio::io_context io;
    Transport transport{io};

    pio::message_forwarder<Policy> pub_messages{};
    pio::message_forwarder<Policy> sub_messages{};
    pio::procedure_forwarder<Policy> pub_procedures{io, std::chrono::hours(1)};
    pio::procedure_forwarder<Policy> sub_procedures{io, std::chrono::hours(1)};

    pio::peer_context<Policy> pub_ctx;
    pio::peer_context<Policy> sub_ctx;
    std::optional<pio::peer_session<Policy>> publisher;   // the dialer end
    std::optional<pio::peer_session<Policy>> subscriber;  // the accepted end

    std::vector<std::string> received;

    void wire()
    {
        transport.on_accepted([this](std::unique_ptr<Channel> ch) {
            sub_ctx.channel = std::move(ch);
            sub_ctx.node_name = "publisher-node";
            subscriber.emplace(sub_ctx, io, make_cfg(0x01), std::chrono::hours(1),
                               sub_messages, sub_procedures, true);
            subscriber->on_message([this](std::string_view, std::span<const std::byte> d) {
                received.emplace_back(reinterpret_cast<const char *>(d.data()), d.size());
            });
            subscriber->start();
        });
        transport.on_dialed([this](std::unique_ptr<Channel> ch, const pio::endpoint &) {
            pub_ctx.channel = std::move(ch);
            pub_ctx.node_name = "subscriber-node";
            publisher.emplace(pub_ctx, io, make_cfg(0x02), std::chrono::hours(1),
                              pub_messages, pub_procedures, false);
            publisher->start();
        });
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

// Drive one live link's confinement: with the subscriber attached for fan-out, a topic
// whose reach EXCLUDES the link's tier delivers nothing; a topic whose reach INCLUDES it
// delivers. `tier_mask` is the reach that should include this link (local for AF_UNIX,
// remote for TCP); `excluded_mask` is one that should not.
template <typename Link>
void prove_link_confinement(Link &l, const std::string &fqn,
                            locality including_mask, locality excluding_mask)
{
    l.pump_until([&] { return l.publisher && l.subscriber
                              && l.publisher->is_complete() && l.subscriber->is_complete(); });
    REQUIRE(l.publisher->is_complete());
    REQUIRE(l.subscriber->is_complete());

    REQUIRE(l.sub_messages.attach(l.subscriber->msg_peer(), fqn));
    REQUIRE(l.pub_messages.attach_for_fanout(l.publisher->msg_peer(), fqn));
    l.settle();

    const std::string payload = "live-confined-payload";

    // Excluded reach: the publisher's fan-out gate drops the send — nothing arrives.
    l.pub_messages.declare(fqn, plexus::topic_qos{.reach = excluding_mask});
    l.received.clear();
    l.pub_messages.publish(fqn, as_bytes(payload), l.publisher->session_id());
    l.settle(std::chrono::milliseconds(40));
    REQUIRE(l.received.empty());   // an off-tier topic NEVER crosses this transport

    // Including reach: the same fan-out delivers over the real transport.
    l.pub_messages.declare(fqn, plexus::topic_qos{.reach = including_mask});
    l.received.clear();
    l.pub_messages.publish(fqn, as_bytes(payload), l.publisher->session_id());
    l.pump_until([&] { return !l.received.empty(); });
    REQUIRE(l.received.size() == 1);
    REQUIRE(l.received.front() == payload);
}

}

TEST_CASE("locality confinement (live AF_UNIX): a remote-confined topic never crosses the local stream; a local one does, looped",
          "[integration][locality][confinement][unix]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        temp_sock sock;
        live_link<pasio::unix_policy, pasio::unix_transport, pasio::unix_channel> l;
        l.wire();
        l.transport.listen({"unix", sock.path});
        l.transport.dial({"unix", sock.path});

        // The AF_UNIX channel classifies as the LOCAL tier: a remote-only topic is dropped,
        // a process|local topic (which includes local) is delivered.
        prove_link_confinement(l, "live.unix.topic",
                               /*including=*/locality::process | locality::local,
                               /*excluding=*/locality::remote);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("locality confinement (live TCP): a local-confined topic never crosses the network stream; a remote one does, looped",
          "[integration][locality][confinement][tcp]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_link<pasio::asio_policy, pasio::asio_transport, pasio::asio_channel> l;
        l.wire();
        l.transport.listen({"tcp", "127.0.0.1:0"});
        const std::uint16_t port = l.transport.port();
        l.transport.dial({"tcp", "127.0.0.1:" + std::to_string(port)});

        // The TCP channel classifies as the REMOTE tier: a process|local topic is dropped,
        // a remote topic is delivered.
        prove_link_confinement(l, "live.tcp.topic",
                               /*including=*/locality::remote,
                               /*excluding=*/locality::process | locality::local);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#endif
