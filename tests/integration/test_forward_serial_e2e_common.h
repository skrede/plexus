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
#include <optional>
#include <algorithm>
#include <string_view>

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

    pasio::serial_transport origin_serial{io};
    pasio::serial_transport relay_serial{io};
    pasio::asio_transport relay_tcp{io};
    pasio::asio_transport consumer_tcp{io};

    node_id origin_id{id_of(0x01)};
    node_id relay_id{id_of(0x02)};
    node_id consumer_id{id_of(0x03)};

    std::optional<origin_node> origin;
    relay_node relay;
    consumer_node consumer;

    std::uint16_t tcp_port{0};

    cold_cluster()
            : relay{io, rdisc, relay_id, relay_serial, relay_tcp, named("relay")}
            , consumer{io, cdisc, consumer_id, consumer_tcp, named("consumer")}
    {
        origin.emplace(io, odisc, origin_id, origin_serial, named("origin"));
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
        relay.dial({"serial", slave_device + "@115200"});
        serial_fixture::settle(io, std::chrono::milliseconds(50));
        origin->router().registry().accept_session(adopt_channel(io, pty.take_master()));
        pump([&] { return connected(relay, origin_id) && connected(*origin, relay_id); });
    }

    // Session B: the relay listens on an ephemeral TCP port; the consumer dials the resolved port. The
    // relay lifts the origin as an origin and replays its peer_report onto the fresh session.
    void bring_up_tcp()
    {
        relay.listen({"tcp", "127.0.0.1:0"});
        tcp_port = relay_tcp.port();
        consumer.dial({"tcp", "127.0.0.1:" + std::to_string(tcp_port)});
        pump([&] { return connected(consumer, relay_id) && connected(relay, consumer_id); });
    }

    // Drain the origin's pending executor work before dropping it, so no posted callback outlives the
    // node it captured; the relay then observes the serial drop and withdraws the relayed peer.
    void kill_origin()
    {
        serial_fixture::settle(io, std::chrono::milliseconds(20));
        origin.reset();
    }

    // The borrowed-executor contract: drain before the nodes are destroyed so no in-flight callback
    // references a torn-down engine.
    ~cold_cluster()
    {
        origin.reset();
        serial_fixture::settle(io, std::chrono::milliseconds(30));
    }
};

}

#endif
