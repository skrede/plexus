// The delivery-edge dedup + origin-override oracle for the forwarded receive half. Two proofs, both
// through a REAL complete peer_session's inject_receive (not a synthesized router):
//
//   1. The same encoded forwarded unidirectional frame injected TWICE delivers exactly ONCE, and the
//      delivered message_info source is the trailer ORIGIN — not the relay session peer the frame
//      arrived over. A forwarded frame carrying a source_identity counter reconstructs its gid against
//      the origin, so the override is observable on the gid's node half.
//   2. A duplicated forwarded rpc_request executes its served handler EXACTLY once — the dedup admits a
//      non-idempotent request a single time regardless of replay.
//
// The relay leg is the inproc session_link's completed handshake; the forwarded frames are injected
// directly, so the dedup state (shared per (origin, arrival-relay) in the receiving forwarder) is what
// gates the second copy.

#include "test_peer_session_inproc_common.h"

#include "plexus/io/message_info.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/inproc/inproc_channel.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using namespace peer_session_inproc_fixture;
namespace wire = plexus::wire;
using plexus::node_id;

namespace {

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

// A real source-identity unidirectional frame (header-on), captured off the wire from a producer that
// declares the topic with emit_source_identity: it carries the gid counter the receive path pairs with
// the delivered source to reconstruct the publisher_gid — the exact shape a relay would splice inside a
// forwarded envelope.
std::vector<std::byte> capture_source_identity_frame(std::string_view fqn, std::string_view body)
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex(bus);
    plexus::inproc::inproc_channel<> sub(ex);
    plexus::inproc::inproc_channel<> capture(ex);
    sub.connect_to(capture.local_endpoint());

    std::vector<std::byte> framed;
    capture.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    plexus::log::null_logger sink;
    msg_forwarder producer{sink};
    producer.declare(fqn, plexus::topic_qos{}, std::nullopt, /*emit_source_identity=*/true);
    producer.attach_for_fanout(typename msg_forwarder::peer{sub, "cap"}, fqn);
    ex.drain();
    producer.publish(fqn, as_bytes(std::string{body}));
    ex.drain();
    return framed;
}

std::vector<std::byte> make_rpc_request_frame(std::string_view fqn, std::string_view param)
{
    wire::bidirectional_header hdr{.source         = wire::endpoint_source_type::caller,
                                   .sequence       = 1,
                                   .topic_hash     = wire::fqn_topic_hash(fqn),
                                   .type_hash_1    = 0,
                                   .type_hash_2    = 0,
                                   .correlation_id = 7};
    const auto rpc_body = wire::encode_rpc_request(hdr, as_bytes(std::string{param}));
    wire::frame_header fhdr{.type = wire::msg_type::rpc_request, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = rpc_body.size()};
    return wire::encode_frame(fhdr, rpc_body);
}

// Wrap a header-on inner frame in a forwarded envelope and frame it for the wire (outer session_id 0 so
// the receiving session's staleness gate lets it through regardless of the latched epoch).
std::vector<std::byte> forwarded_frame_of(const node_id &origin, std::uint16_t seq, std::span<const std::byte> inner)
{
    wire::forwarded_frame ff;
    ff.origin      = origin;
    ff.destination = id_of(0x01); // the receiving node
    ff.hop         = 1;
    ff.seq         = seq;
    ff.flags       = wire::k_forwarded_relay_consent_flag;
    ff.inner.assign(inner.begin(), inner.end());

    const auto payload = wire::encode_forwarded_frame(ff);
    wire::frame_header fhdr{.type = wire::msg_type::forwarded, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = payload.size()};
    return wire::encode_frame(fhdr, payload);
}

}

TEST_CASE("forwarded_dedup_delivery: a double-injected forwarded frame delivers once with the origin as source", "[integration][forwarded][dedup]")
{
    session_link link;
    link.drive();
    REQUIRE(link.responder->is_complete());

    // The receiving node resolves the topic (registers topic_hash -> fqn) so the forwarded unidirectional
    // is deliverable rather than dropped topic_unknown.
    link.resp_messages.attach(link.responder->msg_peer(), "relayed.topic");
    link.drive();

    std::size_t deliveries = 0;
    std::optional<plexus::io::message_info> last;
    link.responder->on_message_with_info([&](std::string_view, std::span<const std::byte>, const plexus::io::message_info &info)
                                         {
                                             ++deliveries;
                                             last = info;
                                         });

    const node_id origin = id_of(0xB0);
    const auto inner     = capture_source_identity_frame("relayed.topic", "relayed-body");
    REQUIRE_FALSE(inner.empty());
    const auto frame = forwarded_frame_of(origin, /*seq=*/100, inner);

    link.responder->inject_receive(frame);
    link.responder->inject_receive(frame); // the replay
    link.drive();

    REQUIRE(deliveries == 1); // dedup admitted the frame exactly once
    REQUIRE(last.has_value());
    REQUIRE(last->source_identity.has_value());
    // The delivered source is the trailer ORIGIN, not the relay session peer (id 0x02).
    REQUIRE(last->source_identity->node_id() == origin);
    REQUIRE(last->source_identity->node_id() != link.responder->peer_identity());
}

TEST_CASE("forwarded_dedup_delivery: a duplicated forwarded rpc_request executes its handler exactly once", "[integration][forwarded][dedup]")
{
    session_link link;
    link.drive();
    REQUIRE(link.responder->is_complete());

    std::size_t handler_calls = 0;
    link.resp_procedures.serve("relayed.rpc",
                               [&](std::span<const std::byte>, rpc_forwarder::reply_fn &reply)
                               {
                                   ++handler_calls;
                                   reply(wire::rpc_status::success, {});
                               });

    const node_id origin = id_of(0xC0);
    const auto inner     = make_rpc_request_frame("relayed.rpc", "arg");
    const auto frame     = forwarded_frame_of(origin, /*seq=*/200, inner);

    link.responder->inject_receive(frame);
    link.responder->inject_receive(frame); // the replay: a non-idempotent request must not re-run
    link.drive();

    REQUIRE(handler_calls == 1);
}
