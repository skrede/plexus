#ifndef HPP_GUARD_TESTS_INTEGRATION_FORWARD_RELAY_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_FORWARD_RELAY_COMMON_H

// A deterministic relayed request/response substrate over one inproc bus: leaf nodes with NO direct
// session to each other exchange a request and its response entirely through an intermediary that holds
// no per-correlation state. Each node is a full routing_engine; a leaf learns the far leaf as a via-only
// candidate by ingesting a hand-built peer_report attributed to the relay (the awareness a live relay
// would emit), then route_select over that candidate wraps the call in a forwarded envelope onto the
// relay session. The relay re-resolves each transiting frame by its destination identity — request and
// reply alike — so nothing but the leaves ever correlate the exchange.

#include "test_routing_engine_inproc_common.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/peer_report.h"

#include <span>
#include <string>
#include <cstdint>

namespace forward_relay_fixture {

namespace wire = plexus::wire;

using routing_inproc_fixture::engine;
using rpc_forwarder = plexus::io::procedure_forwarder<routing_inproc_fixture::manual_policy>;
using routing_inproc_fixture::manual_clock;
using routing_inproc_fixture::transport_t;
using routing_inproc_fixture::make_cfg;
using routing_inproc_fixture::make_id;
using routing_inproc_fixture::forever_cfg;
using routing_inproc_fixture::k_long_timeout;
using routing_inproc_fixture::k_seed;
using routing_inproc_fixture::as_bytes;
using routing_inproc_fixture::to_string;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::io::endpoint;
using plexus::node_id;

// A via-only awareness report: the relay tells `to` it can reach `origin`. seq lets a later withdrawal
// (the same origin/reporter, a higher seq, the withdrawal flag) retire the row it installed.
inline wire::peer_report reach_report(const node_id &origin, std::uint16_t seq = 1, std::uint8_t flags = 0)
{
    wire::peer_report pr{};
    pr.origin          = origin;
    pr.origin_universe = plexus::discovery::k_default_universe;
    pr.hop             = 1;
    pr.seq             = seq;
    pr.flags           = flags;
    return pr;
}

// One relay wired to two leaves that have no direct path to each other. The relay dials both leaves
// (their sessions are the relay's direct candidates); each leaf then ingests the relay's report of the
// other leaf so its only route across is the via-relay candidate.
struct relay_line
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t t_caller{ex, bus};
    transport_t t_relay{ex, bus};
    transport_t t_server{ex, bus};
    plexus::log::null_logger sink;

    engine caller;
    engine relay;
    engine server;

    node_id id_caller{make_id(0xC1)};
    node_id id_relay{make_id(0x2A)};
    node_id id_server{make_id(0x53)};
    endpoint ep_caller{"inproc", "reqres-caller"};
    endpoint ep_relay{"inproc", "reqres-relay"};
    endpoint ep_server{"inproc", "reqres-server"};

    relay_line()
            : caller(t_caller, ex, make_cfg(0xC1), k_long_timeout, forever_cfg(), k_seed, sink)
            , relay(t_relay, ex, make_cfg(0x2A), k_long_timeout, forever_cfg(), k_seed, sink)
            , server(t_server, ex, make_cfg(0x53), k_long_timeout, forever_cfg(), k_seed, sink)
    {
        caller.listen(ep_caller);
        relay.listen(ep_relay);
        server.listen(ep_server);
        relay.note_peer(id_caller, ep_caller);
        relay.note_peer(id_server, ep_server);
        relay.reach(id_caller);
        relay.reach(id_server);
        drive();
    }

    void drive()
    {
        ex.drain();
    }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        drive();
    }
};

}

#endif
