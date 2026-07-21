// The mixed-version oracle for the forwarded verb: proof that an encoded forwarded frame is
// structurally unparseable-as-payload by the REAL shipped v0.3.0 parser, not a simulation of it.
//
// The harness is a BARE frame_router carrying ONLY the pre-forwarded (v0.3.0) consumer set,
// hand-registered — deliberately NOT the full session consumer set — so a router with no forwarded
// consumer installed models the v0.3.0 surface. A 0x0F frame therefore fires no consumer and is
// warn-dropped whole (at the shipped default arm before the dispatch arm exists; as an
// unregistered-consumer drop after), delivering nothing and leaving the session healthy. This is the
// fail-closed carrier property: a flag-bit or appended trailer would instead ride inside the
// delivered unidirectional payload (decode_unidirectional returns data = reader.rest()).

#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <span>
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
// consumers that ran (empty == nothing delivered), not merely on the absence of a crash.
struct fired_set
{
    std::set<wire::msg_type> types;

    plexus::io::frame_router::consumer record(wire::msg_type t)
    {
        return [this, t](std::span<const std::byte>) { types.insert(t); };
    }
};

// The pre-forwarded (v0.3.0) consumer set, hand-registered: the shipped surface a v0.3.0 node exposes
// has NO forwarded consumer, so this models it directly.
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

node_id fill(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

std::vector<std::byte> make_forwarded_payload()
{
    // A header-on inner frame: a real encoded unidirectional frame nested inside the envelope, the
    // exact shape a v0.3.0 node would silently deliver were this a rider rather than a new verb.
    wire::frame_header inner_hdr{.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 4};
    const std::vector<std::byte> inner_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
    const auto inner = wire::encode_frame(inner_hdr, inner_payload);

    wire::forwarded_frame ff;
    ff.origin      = fill(0xB0);
    ff.destination = fill(0xC0);
    ff.hop         = 1;
    ff.seq         = 42;
    ff.flags       = wire::k_forwarded_relay_consent_flag;
    ff.inner.assign(inner.begin(), inner.end());
    return wire::encode_forwarded_frame(ff);
}

std::vector<std::byte> frame_of(wire::msg_type type, std::span<const std::byte> payload)
{
    wire::frame_header hdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = payload.size()};
    return wire::encode_frame(hdr, payload);
}

}

TEST_CASE("forwarded oracle: a 0x0F frame fires no consumer and delivers nothing against the v0.3.0 router", "[integration][forwarded][oracle]")
{
    counting_logger sink;
    plexus::io::frame_router router{sink};
    fired_set fired;
    register_v0_3_0_consumers(router, fired);

    // Control: the harness genuinely dispatches — a declare frame fires exactly its consumer. Without
    // this, "no consumer fired" for the forwarded verb could pass on a dead harness.
    router.route(frame_of(wire::msg_type::declare, std::vector<std::byte>{}));
    REQUIRE(fired.types == std::set<wire::msg_type>{wire::msg_type::declare});
    fired.types.clear();

    const auto payload      = make_forwarded_payload();
    const auto drops_before = sink.drops;
    router.route(frame_of(wire::msg_type::forwarded, payload));

    REQUIRE(fired.types.empty());            // nothing delivered: no consumer ran
    REQUIRE(sink.drops == drops_before + 1); // the frame was warn-dropped whole

    // The session survives: a subsequent well-formed frame still dispatches normally.
    router.route(frame_of(wire::msg_type::declare, std::vector<std::byte>{}));
    REQUIRE(fired.types == std::set<wire::msg_type>{wire::msg_type::declare});
}

TEST_CASE("forwarded oracle: the router dispatches its arm when a forwarded consumer is installed", "[integration][forwarded][oracle]")
{
    counting_logger sink;
    plexus::io::frame_router router{sink};
    fired_set fired;
    register_v0_3_0_consumers(router, fired);
    router.on_forwarded(fired.record(wire::msg_type::forwarded));

    const auto payload = make_forwarded_payload();
    router.route(frame_of(wire::msg_type::forwarded, payload));

    REQUIRE(fired.types == std::set<wire::msg_type>{wire::msg_type::forwarded});
}
