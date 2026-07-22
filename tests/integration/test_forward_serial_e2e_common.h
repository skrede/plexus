#ifndef HPP_GUARD_TESTS_INTEGRATION_FORWARD_SERIAL_E2E_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_FORWARD_SERIAL_E2E_COMMON_H

// The two-live-transport relay acceptance substrate, driven entirely through the public node facade.
// A serial-attached origin, a relay owning BOTH a serial and a TCP session, and a TCP-attached
// consumer stand up on ONE shared io_context with NO direct origin<->consumer path. Session A is a
// real openpty serial link: the origin adopts the pty MASTER (which has no device name) as an inbound
// session, and the relay dials the SLAVE by its ptsname. Session B is a real TCP loopback: the relay
// listens on an ephemeral port and the consumer dials it. Everything is reconstructed per cold run,
// so each run is an independent cold establishment of BOTH sessions rather than one warm relay polled
// repeatedly.

#include "test_serial_common.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"
#include "plexus/target_profile.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/graph/participant_record.h"

#include <asio/io_context.hpp>

#include <cstdlib>

#include <span>
#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>
#include <type_traits>

namespace forward_serial_e2e_fixture {

namespace pasio = plexus::asio;
namespace graph = plexus::graph;
using plexus::node_id;
using serial_fixture::pty_pair;
using serial_fixture::adopt_channel;
using serial_fixture::pump_until;

using origin_node   = plexus::node<pasio::serial_policy, pasio::serial_transport>;
using relay_node    = plexus::node<plexus::relay<pasio::asio_policy>, pasio::serial_transport, pasio::asio_transport>;
using consumer_node = plexus::node<pasio::asio_policy, pasio::asio_transport>;

inline node_id id_of(std::uint8_t seed)
{
    node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Records how many warn lines the node emitted and, of those, how many were self_check() posture
// disclosures (tagged "(self_check)" by node.h) — so a test proves relay-posture disclosure is never
// silent without coupling to the exact wording.
struct capturing_logger final : plexus::log::logger
{
    void warn(std::string_view message) override
    {
        ++count;
        if(message.find("self_check") != std::string_view::npos)
            ++self_checks;
    }
    std::size_t count{0};
    std::size_t self_checks{0};
};

// A complete session whose HANDSHAKE-PROVEN identity is `proven` exists (slots key by endpoint hash,
// not the proven id, so connectivity is asserted by peer_identity, never a keyed lookup).
template<typename Node>
inline bool connected(Node &n, const node_id &proven)
{
    bool found = false;
    n.router().registry().for_each_connected([&](const node_id &, auto &s) { if(s.peer_identity() == proven) found = true; });
    return found;
}

inline const graph::participant_record *find_participant(const consumer_node &c, const node_id &id)
{
    std::array<graph::participant_record, 8> buf{};
    const auto res = c.participants(buf);
    const std::span<const graph::participant_record> parts{buf.data(), res.count};
    static graph::participant_record hit{};
    const auto it = std::find_if(parts.begin(), parts.end(), [&](const auto &r) { return r.id == id; });
    if(it == parts.end())
        return nullptr;
    hit = *it;
    return &hit;
}

// Three fresh nodes plus their borrowed substrate, reconstructed per run. bring_up_serial and
// bring_up_tcp establish the two live sessions cold; kill_origin tears the origin down so the relay
// withdraws the relayed peer over session B.
struct cold_cluster
{
    ::asio::io_context io;

    pty_pair pty;
    std::string slave_device{::ptsname(pty.master)};

    plexus::discovery::static_discovery odisc{{}};
    plexus::discovery::static_discovery rdisc{{}};
    plexus::discovery::static_discovery cdisc{{}};
    plexus::discovery::static_discovery o2disc{{}};

    pasio::serial_transport origin_serial{io};
    pasio::serial_transport relay_serial{io};
    pasio::asio_transport relay_tcp{io};
    pasio::asio_transport consumer_tcp{io};
    pasio::asio_transport origin2_tcp{io};

    node_id origin_id{id_of(0x01)};
    node_id relay_id{id_of(0x02)};
    node_id consumer_id{id_of(0x03)};
    node_id origin2_id{id_of(0x04)};

    capturing_logger relay_log;
    capturing_logger consumer_log;

    std::optional<origin_node> origin;
    std::optional<relay_node> relay;
    consumer_node consumer;
    std::optional<consumer_node> origin2;

    std::uint16_t tcp_port{0};

    explicit cold_cluster(plexus::io::route_usage consumer_usage = plexus::io::route_usage::prefer_direct,
                          plexus::io::route_usage origin_usage = plexus::io::route_usage::prefer_direct)
            : consumer{io, cdisc, consumer_id, consumer_tcp, logged(usage_options("consumer", consumer_usage), consumer_log)}
    {
        relay.emplace(io, rdisc, relay_id, relay_serial, relay_tcp, logged(named("relay"), relay_log));
        origin.emplace(io, odisc, origin_id, origin_serial, usage_options("origin", origin_usage));
    }

    static plexus::node_options logged(plexus::node_options opts, capturing_logger &log)
    {
        opts.logger = &log;
        return opts;
    }

    static plexus::node_options usage_options(std::string_view n, plexus::io::route_usage usage)
    {
        plexus::node_options opts = named(n);
        opts.routes.usage         = usage;
        return opts;
    }

    static plexus::node_options named(std::string_view n)
    {
        using namespace std::chrono_literals;
        plexus::node_options opts;
        opts.name            = std::string{n};
        opts.reconnect       = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
        std::uint64_t seed   = 0x9E3779B97F4A7C15ull;
        for(char c : n)
            seed = (seed ^ static_cast<std::uint8_t>(c)) * 1099511628211ull;
        opts.redial_seed     = seed;
        opts.handshake_retry = 8;
        return opts;
    }

    template<typename Pred>
    void pump(Pred pred)
    {
        pump_until(io, pred);
    }

    // Session A: the relay dials the slave by its ptsname (the slave is the only path-addressable end),
    // THEN the origin adopts the pty master as an inbound session. The order matters: a master read armed
    // while no slave is open reads EOF and tears the master channel down, so the slave must be open first.
    // The openpty slave fd is released so the relay owns the sole slave handle.
    void bring_up_serial()
    {
        ::close(pty.take_slave());
        relay->dial({"serial", slave_device + "@115200"});
        serial_fixture::settle(io, std::chrono::milliseconds(50));
        origin->router().registry().accept_session(adopt_channel(io, pty.take_master()));
        pump([&] { return connected(*relay, origin_id) && connected(*origin, relay_id); });
    }

    // Session B: the relay listens on an ephemeral TCP port; the consumer dials the resolved port. The
    // relay lifts the origin as an origin and replays its peer_report onto the fresh session.
    void bring_up_tcp()
    {
        relay->listen({"tcp", "127.0.0.1:0"});
        tcp_port = relay_tcp.port();
        consumer.dial({"tcp", "127.0.0.1:" + std::to_string(tcp_port)});
        pump([&] { return connected(consumer, relay_id) && connected(*relay, consumer_id); });
    }

    // A second, non-declining origin dials the relay's TCP listener so the relay lifts it as a reported
    // origin — the positive control that a declining origin's absence downstream is the decline, not a
    // broken fixture. Requires bring_up_tcp first (the relay's port is resolved there).
    void bring_up_origin2()
    {
        origin2.emplace(io, o2disc, origin2_id, origin2_tcp, named("origin2"));
        origin2->dial({"tcp", "127.0.0.1:" + std::to_string(tcp_port)});
        pump([&] { return connected(*origin2, relay_id) && connected(*relay, origin2_id); });
    }

    // Drain the origin's pending executor work before dropping it, so no posted callback outlives the
    // node it captured; the relay then observes the serial drop and withdraws the relayed peer.
    void kill_origin()
    {
        serial_fixture::settle(io, std::chrono::milliseconds(20));
        origin.reset();
    }

    // Destroy the WHOLE relay node — both its sessions — draining pending work first. Unlike a
    // serial-leg-down (which leaves the relay alive to withdraw the reported origin, retiring it to
    // no_provider), tearing the relay session down leaves the origin's via-relay candidate stale in
    // the consumer's route table, so a forwarded re-issue finds no live via-session and drops.
    void kill_relay()
    {
        serial_fixture::settle(io, std::chrono::milliseconds(20));
        relay.reset();
    }

    // The borrowed-executor contract: drain before the nodes are destroyed so no in-flight callback
    // references a torn-down engine.
    ~cold_cluster()
    {
        origin.reset();
        origin2.reset();
        serial_fixture::settle(io, std::chrono::milliseconds(30));
    }
};

using host_node      = consumer_node;
using tcp_relay_node = plexus::node<plexus::relay<pasio::asio_policy>, pasio::asio_transport>;
using session_ref    = std::remove_reference_t<decltype(*std::declval<host_node &>().router().registry().session_for(node_id{}))>;

// An all-TCP dual-homed substrate: origin, relay, and consumer are each an asio_transport node on one
// shared io_context. The consumer's single transport dials the relay (the relayed path) AND, later, the
// origin directly, so it is genuinely dual-homed to the one origin identity — the topology a
// serial-attached origin structurally cannot host. The relay's single transport both listens (the
// consumer dials in) and dials the origin, lifting it as a reported origin over the consumer's session.
struct dualhome_cluster
{
    ::asio::io_context io;

    plexus::discovery::static_discovery odisc{{}};
    plexus::discovery::static_discovery rdisc{{}};
    plexus::discovery::static_discovery cdisc{{}};

    pasio::asio_transport origin_tcp{io};
    pasio::asio_transport relay_tcp{io};
    pasio::asio_transport consumer_tcp{io};

    node_id origin_id{id_of(0x01)};
    node_id relay_id{id_of(0x02)};
    node_id consumer_id{id_of(0x03)};

    std::optional<host_node> origin;
    std::optional<tcp_relay_node> relay;
    host_node consumer;

    std::uint16_t origin_port{0};
    std::uint16_t relay_port{0};

    dualhome_cluster()
            : consumer{io, cdisc, consumer_id, consumer_tcp, cold_cluster::named("consumer")}
    {
        relay.emplace(io, rdisc, relay_id, relay_tcp, cold_cluster::named("relay"));
        origin.emplace(io, odisc, origin_id, origin_tcp, cold_cluster::named("origin"));
        origin->listen({"tcp", "127.0.0.1:0"});
        origin_port = origin_tcp.port();
    }

    template<typename Pred>
    void pump(Pred pred)
    {
        pump_until(io, pred);
    }

    // The relay dials the origin and lifts it, replaying its report onto any consumer session.
    void bring_up_relay_to_origin()
    {
        relay->dial({"tcp", "127.0.0.1:" + std::to_string(origin_port)});
        pump([&] { return connected(*relay, origin_id) && connected(*origin, relay_id); });
    }

    // The consumer dials the relay: it reaches the origin over the relayed path only.
    void bring_up_consumer_to_relay()
    {
        relay->listen({"tcp", "127.0.0.1:0"});
        relay_port = relay_tcp.port();
        consumer.dial({"tcp", "127.0.0.1:" + std::to_string(relay_port)});
        pump([&] { return connected(consumer, relay_id) && connected(*relay, consumer_id); });
    }

    // The consumer dials the origin directly on the SAME transport carrying its relay session: now one
    // live direct AND one live relayed path resolve to the one origin identity.
    void bring_up_consumer_to_origin_direct()
    {
        consumer.dial({"tcp", "127.0.0.1:" + std::to_string(origin_port)});
        pump([&] { return connected(consumer, origin_id) && connected(*origin, consumer_id); });
    }

    // Tear down ONLY the consumer's direct session to the origin, leaving consumer<->relay and
    // relay<->origin live so the relayed path re-arms. tear_down fires the disconnected edge the re-arm
    // keys on without disturbing the other two sessions.
    void drop_direct()
    {
        session_ref *direct = nullptr;
        consumer.router().registry().for_each_connected([&](const node_id &, session_ref &s) { if(s.peer_identity() == origin_id) direct = &s; });
        if(direct != nullptr)
            direct->tear_down();
        pump([&] { return !connected(consumer, origin_id); });
    }

    ~dualhome_cluster()
    {
        origin.reset();
        relay.reset();
        serial_fixture::settle(io, std::chrono::milliseconds(30));
    }
};

}

#endif
