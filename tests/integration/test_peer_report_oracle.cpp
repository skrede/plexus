// The mixed-version oracle: proof that an encoded peer_report frame is structurally
// unparseable-as-direct by the REAL shipped v0.3.0 parser, not a simulation of it.
//
// The harness is a BARE frame_router carrying ONLY the pre-peer_report (v0.3.0) consumer set,
// hand-registered — deliberately NOT register_session_consumers, so that when a later plan installs
// an always-compiled peer_report consumer through that function this oracle keeps proving the
// v0.3.0-shaped behavior: a router with no peer_report consumer registered. A 0x0E frame therefore
// fires no consumer and is warn-dropped whole (at the shipped default arm before the dispatch arm
// exists; as an unregistered-consumer drop after), producing no peer and leaving the session
// healthy. A genuinely-unknown type (0x7F) permanently pins the default warn-drop arm. The magic
// gate is proven directly: decode_announcement over the same bytes is nullopt.

#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/announcement.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::node_id;
namespace wire = plexus::wire;

namespace {

// warn() bumps a counter — proves the warn-and-drop seam fired without coupling to the message text.
struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view) override
    {
        ++drops;
    }
    std::size_t drops{0};
};

// Records which msg_type each fired consumer carried, so the oracle asserts on the EXACT set of
// consumers that ran (empty == no peer produced), not merely on the absence of a crash.
struct fired_set
{
    std::set<wire::msg_type> types;

    plexus::io::frame_router::consumer record(wire::msg_type t)
    {
        return [this, t](std::span<const std::byte>) { types.insert(t); };
    }
};

// The pre-peer_report (v0.3.0) consumer set, hand-registered. This is the shipped surface a v0.3.0
// node exposes — NO peer_report consumer — and is intentionally installed WITHOUT
// register_session_consumers so a later consumer install through that function cannot reach here.
void register_v0_3_0_consumers(plexus::io::frame_router &router, fired_set &fired)
{
    router.on_subscribe(fired.record(wire::msg_type::subscribe));
    router.on_fetch_latched(fired.record(wire::msg_type::fetch_latched));
    router.on_unsubscribe(fired.record(wire::msg_type::unsubscribe));
    router.on_subscribe_response(fired.record(wire::msg_type::subscribe_response));
    router.on_rpc_request(fired.record(wire::msg_type::rpc_request));
    router.on_rpc_response(fired.record(wire::msg_type::rpc_response));
    router.on_handshake_req(fired.record(wire::msg_type::handshake_req));
    router.on_handshake_resp(fired.record(wire::msg_type::handshake_resp));
    router.on_heartbeat(fired.record(wire::msg_type::heartbeat));
    router.on_declare(fired.record(wire::msg_type::declare));
    router.on_unidirectional([&fired](const wire::frame_header &, std::span<const std::byte>) { fired.types.insert(wire::msg_type::unidirectional); });
}

node_id make_origin()
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(0xB0 + i);
    return id;
}

std::vector<std::byte> make_peer_report_payload()
{
    wire::peer_report pr;
    pr.origin          = make_origin();
    pr.origin_universe = 0x11223344u;
    pr.hop             = 1;
    pr.seq             = 42;
    pr.flags           = wire::k_peer_report_consent_flag | wire::k_peer_report_topics_flag;
    pr.topics          = {wire::topic_declaration{.topic_hash = 7, .type_id = 9, .fqn = "sensor/imu", .type_name = "geometry/Pose", .state = wire::type_state::declared}};
    return wire::encode_peer_report(pr);
}

std::vector<std::byte> frame_of(wire::msg_type type, std::span<const std::byte> payload)
{
    wire::frame_header hdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = payload.size()};
    return wire::encode_frame(hdr, payload);
}

}

TEST_CASE("peer_report oracle: a 0x0E frame fires no consumer and produces no peer against the v0.3.0 router", "[integration][peer_report][oracle]")
{
    counting_logger sink;
    plexus::io::frame_router router{sink};
    fired_set fired;
    register_v0_3_0_consumers(router, fired);

    // Control: the harness genuinely dispatches — a declare frame fires exactly its consumer. Without
    // this, "no consumer fired" for 0x0E could pass on a dead harness (Pitfall 1).
    router.route(frame_of(wire::msg_type::declare, std::vector<std::byte>{}));
    REQUIRE(fired.types == std::set<wire::msg_type>{wire::msg_type::declare});
    fired.types.clear();

    const auto payload = make_peer_report_payload();
    const auto drops_before = sink.drops;
    router.route(frame_of(wire::msg_type::peer_report, payload));

    REQUIRE(fired.types.empty());          // no consumer ran: no peer produced
    REQUIRE(sink.drops == drops_before + 1); // the frame was warn-dropped whole
}

TEST_CASE("peer_report oracle: the peer_report bytes fail the announcement magic gate", "[integration][peer_report][oracle]")
{
    const auto payload = make_peer_report_payload();
    REQUIRE(!wire::decode_announcement(payload).has_value());
}

TEST_CASE("peer_report oracle: a genuinely-unknown type warn-drops at the default arm", "[integration][peer_report][oracle]")
{
    counting_logger sink;
    plexus::io::frame_router router{sink};
    fired_set fired;
    register_v0_3_0_consumers(router, fired);

    // 0x7F has no assigned verb, so it reaches the shipped default: — a permanent pin on the
    // unknown-verb warn-drop, held far from the assignment frontier and durable independent of the
    // now-handled status of any lower value.
    const auto unknown = static_cast<wire::msg_type>(0x7F);
    const auto drops_before = sink.drops;
    router.route(frame_of(unknown, std::vector<std::byte>{}));

    REQUIRE(fired.types.empty());
    REQUIRE(sink.drops == drops_before + 1);
}
