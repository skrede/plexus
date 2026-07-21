// The relay's report-emission gate, host-side over a real openpty serial link. A serial-attached origin
// declares a topic-with-type that folds over the REAL serial wire into the relay's topic table; the relay
// lifts the handshake-proven identity + folded topics through its production report emitter (real emitter +
// wire codec) into a peer_report, and a kill-origin drop emits the matching withdrawal. This TU covers the
// relay's real-wire lift and report/withdrawal EMISSION; the downstream ingestion + enumeration of that
// report ACROSS a second live transport is covered end-to-end by the two-live-transport acceptance, so no
// report is hand-delivered here. Looped in-body; the ctest invocation is re-run >=3 process runs (the
// serial-claim discipline).

#include "test_serial_common.h"

#include "plexus/io/report_options.h"
#include "plexus/io/peer_report_emitter.h"

#include "plexus/discovery/universe.h"

#include "plexus/graph/topic_type_table.h"

#include "plexus/wire/peer_report.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

using namespace serial_fixture;

namespace graph = plexus::graph;
namespace wire  = plexus::wire;
using plexus::node_id;
using graph::topic_type_table;
using plexus::io::make_report_ctx;
using plexus::io::report_options;
using plexus::io::peer_report_emitter;

namespace {
const wire::topic_declaration *topic_named(const std::vector<wire::topic_declaration> &topics, std::string_view fqn)
{
    const auto it = std::find_if(topics.begin(), topics.end(), [&](const auto &t) { return t.fqn == fqn; });
    return it == topics.end() ? nullptr : &*it;
}
}

TEST_CASE("a relay lifts a serial-attached origin's real-wire declaration into a peer_report and withdraws it on kill-origin, looped",
          "[integration][serial][relay][e2e]")
{
    constexpr int k_runs = 5;

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

        // The relay lifts the proven identity + real-wire-folded topics into a peer_report through the
        // production emitter + wire codec — no report is hand-stitched onto a downstream node.
        const node_id origin_id = relay.peer_identity();
        peer_report_emitter                   emitter;
        std::optional<std::vector<std::byte>> asserted;
        emitter.note_origin(make_report_ctx(report_options{}), origin_id, plexus::discovery::k_default_universe, table,
                            [&](const wire::peer_report &pr) { asserted = wire::encode_peer_report(pr); });
        REQUIRE(asserted.has_value());
        const auto reported = wire::decode_peer_report(*asserted);
        REQUIRE(reported.has_value());
        REQUIRE(reported->origin == origin_id);
        const auto *topic = topic_named(reported->topics, "sensor/temp");
        REQUIRE(topic != nullptr);
        REQUIRE(topic->type_name == "Temp");

        // Killing the origin drops the live serial path: the relay withdraws the origin downstream.
        std::optional<std::vector<std::byte>> withdrawn;
        relay.on_transport_drop([&] { emitter.withdraw(origin_id, [&](const wire::peer_report &pr) { withdrawn = wire::encode_peer_report(pr); }); });
        origin_ctx.channel->close();
        pump_until(io, [&] { return withdrawn.has_value(); });
        REQUIRE(withdrawn.has_value());
        const auto retired = wire::decode_peer_report(*withdrawn);
        REQUIRE(retired.has_value());
        REQUIRE(retired->origin == origin_id);
    }
}
