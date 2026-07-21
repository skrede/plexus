// The control-plane acceptance gate, host-side over a real openpty serial link: a consumer node
// enumerates a serial-attached origin THROUGH a relay, end-to-end, before hardware. The origin's
// declaration folds over the REAL serial wire into the relay's topic table; the relay lifts the
// handshake-proven identity + topics and emits a peer_report (real emitter + wire codec, both ways)
// the consumer ingests and surfaces AS relayed beside an unblended direct peer. Killing the origin's
// serial session withdraws it — the consumer retires it with no identity churn. Looped in-body; the
// ctest invocation is re-run >=3 process runs (the serial-claim discipline).

#include "test_serial_common.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/report_options.h"
#include "plexus/io/peer_report_emitter.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/universe.h"
#include "plexus/discovery/static_discovery.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/topic_type_table.h"
#include "plexus/graph/participant_record.h"

#include "plexus/wire/peer_report.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <string_view>

using namespace serial_fixture;

namespace graph = plexus::graph;
namespace wire  = plexus::wire;
using plexus::node_id;
using graph::topic_type_table;
using plexus::io::make_report_ctx;
using plexus::io::report_options;
using plexus::io::peer_report_emitter;

namespace {
node_id make_node_id(std::uint8_t seed)
{
    node_id id{};
    id[0] = std::byte{seed};
    return id;
}
plexus::io::endpoint make_ep(std::string_view addr)
{
    return {"inproc", std::string{addr}};
}

// A stock inproc node: it ingests relayed peer_reports off the wire and enumerates them.
struct consumer
{
    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   ex{bus};
    plexus::inproc::inproc_transport<>  transport{ex, bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::node<plexus::inproc::inproc_policy, plexus::inproc::inproc_transport<>> node;

    explicit consumer(const node_id &id)
            : node{ex, disc, id, transport, plexus::node_options{}}
    {
    }

    void ingest(const node_id &reporter, std::span<const std::byte> bytes)
    {
        const auto pr = wire::decode_peer_report(bytes);
        REQUIRE(pr.has_value());
        node.router().ingest_peer_report(reporter, *pr);
        ex.drain();
    }
};

const graph::participant_record *participant_of(std::span<const graph::participant_record> f, const node_id &id)
{
    const auto it = std::find_if(f.begin(), f.end(), [&](const auto &r) { return r.id == id; });
    return it == f.end() ? nullptr : &*it;
}
const graph::topic_record *topic_of(std::span<const graph::topic_record> f, const node_id &node)
{
    const auto it = std::find_if(f.begin(), f.end(), [&](const auto &r) { return r.node == node; });
    return it == f.end() ? nullptr : &*it;
}
void expect_relayed(const consumer &c, const node_id &origin, const node_id &relay, const node_id &direct)
{
    std::array<graph::participant_record, 8> pbuf{};
    const auto pres = c.node.participants(pbuf);
    const std::span<const graph::participant_record> parts{pbuf.data(), pres.count};
    const auto *reported = participant_of(parts, origin);
    REQUIRE(reported != nullptr);
    REQUIRE(reported->origin.how == graph::observation::reported);
    REQUIRE(reported->origin.reporter == relay);
    REQUIRE(reported->reach.via == relay);
    const auto *peer = participant_of(parts, direct);
    REQUIRE(peer != nullptr);
    REQUIRE(peer->origin.how == graph::observation::directly_observed);
    REQUIRE_FALSE(peer->reach.via.has_value());
    std::array<graph::topic_record, 8> tbuf{};
    const auto tres   = c.node.topics(tbuf);
    const auto *topic = topic_of({tbuf.data(), tres.count}, origin);
    REQUIRE(topic != nullptr);
    REQUIRE(topic->name == "sensor/temp");
    REQUIRE(topic->role == graph::topic_role::publisher);
    REQUIRE(topic->types.count == 1);
    REQUIRE(topic->types.names[0] == "Temp");
}

void expect_retired(const consumer &c, const node_id &origin, const node_id &direct, const plexus::io::endpoint &ep_direct)
{
    std::array<graph::participant_record, 8> pbuf{};
    const auto pres = c.node.participants(pbuf);
    const std::span<const graph::participant_record> parts{pbuf.data(), pres.count};
    REQUIRE(participant_of(parts, origin) == nullptr);
    const auto *peer = participant_of(parts, direct); // the surviving direct identity is untouched
    REQUIRE(peer != nullptr);
    REQUIRE(peer->origin.how == graph::observation::directly_observed);
    REQUIRE(peer->reach.transport == ep_direct);
    std::array<graph::topic_record, 8> tbuf{};
    const auto tres = c.node.topics(tbuf);
    REQUIRE(topic_of({tbuf.data(), tres.count}, origin) == nullptr);
}

}

TEST_CASE("a consumer enumerates a serial-attached origin through a relay end-to-end, then retires it on kill-origin, looped",
          "[integration][serial][relay][e2e]")
{
    constexpr int k_runs = 5;
    const auto relay_id  = make_node_id(0x0A);
    const auto direct_id = make_node_id(0x0B);
    const auto ep_relay  = make_ep("relay:5000");
    const auto ep_direct = make_ep("direct:6000");

    for(int run = 0; run < k_runs; ++run)
    {
        pty_pair pty;
        ::asio::io_context io;
        plexus::log::null_logger sink;
        serial_msg_fwd origin_messages{sink};
        serial_msg_fwd relay_messages{sink};
        serial_rpc_fwd origin_procedures{io, k_long_timeout, sink};
        serial_rpc_fwd relay_procedures{io, k_long_timeout, sink};
        pio::peer_context<pasio::serial_policy> origin_ctx;
        pio::peer_context<pasio::serial_policy> relay_ctx;
        origin_ctx.channel   = adopt_channel(io, pty.take_master());
        origin_ctx.node_name = "relay";
        relay_ctx.channel    = adopt_channel(io, pty.take_slave());
        relay_ctx.node_name  = "origin";
        serial_session origin{origin_ctx, io, make_cfg(0x01), k_long_timeout, origin_messages, origin_procedures, /*is_inbound_bootstrap=*/false, sink};
        serial_session relay{relay_ctx, io, make_cfg(0x02), k_long_timeout, relay_messages, relay_procedures, /*is_inbound_bootstrap=*/true, sink};

        // The origin's declaration folds over the REAL serial wire into the relay's topic table,
        // attributed by the receive consumers to the handshake-proven origin id.
        topic_type_table table;
        bool             folded = false;
        relay_messages.on_topic_edge([&](const graph::topic_edge &e) { table.upsert(e); folded = true; });
        origin_messages.declare("sensor/temp", plexus::topic_qos{}, std::optional<std::uint64_t>{42}, false, "Temp");
        origin.start();
        relay.start();
        pump_until(io, [&] { return origin.is_complete() && relay.is_complete() && folded; });
        REQUIRE(origin.is_complete());
        REQUIRE(relay.is_complete());
        REQUIRE(folded);
        const node_id origin_id = relay.peer_identity();
        peer_report_emitter                   emitter;
        std::optional<std::vector<std::byte>> asserted;
        emitter.note_origin(make_report_ctx(report_options{}), origin_id, plexus::discovery::k_default_universe, table,
                            [&](const wire::peer_report &pr) { asserted = wire::encode_peer_report(pr); });
        REQUIRE(asserted.has_value());
        consumer cons{make_node_id(0x0C)};
        cons.node.router().note_peer(relay_id, ep_relay);
        cons.node.router().note_peer(direct_id, ep_direct);
        cons.ingest(relay_id, *asserted);
        expect_relayed(cons, origin_id, relay_id, direct_id);

        // Killing the origin drops the live serial path: the relay withdraws, the consumer retires.
        std::optional<std::vector<std::byte>> withdrawn;
        relay.on_transport_drop([&] { emitter.withdraw(origin_id, [&](const wire::peer_report &pr) { withdrawn = wire::encode_peer_report(pr); }); });
        origin_ctx.channel->close();
        pump_until(io, [&] { return withdrawn.has_value(); });
        REQUIRE(withdrawn.has_value());
        cons.ingest(relay_id, *withdrawn);
        expect_retired(cons, origin_id, direct_id, ep_direct);
    }
}
