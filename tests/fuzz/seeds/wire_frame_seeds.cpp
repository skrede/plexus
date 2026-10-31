#include "seed_sink.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/announcement.h"
#include "plexus/wire/fetch_latched.h"
#include "plexus/wire/forwarded_frame.h"
#include "plexus/wire/topic_declaration.h"

#include "plexus/node_id.h"

#include <cstddef>
#include <cstdint>

namespace {

using namespace plexus;

constexpr std::uint64_t k_topic = 0x1122334455667788;
constexpr node_id k_origin{std::byte{0x0a}, std::byte{0x0b}, std::byte{0x0c}};
constexpr node_id k_destination{std::byte{0xd0}, std::byte{0xd1}};
constexpr wire::bidirectional_header k_call{
        .source = wire::endpoint_source_type::caller, .sequence = 3, .topic_hash = k_topic, .type_hash_1 = 0, .type_hash_2 = 0, .correlation_id = 41};

template<typename Handshake>
Handshake handshake_fields()
{
    Handshake h{};
    h.id.fill(std::byte{0x11});
    h.version_minor            = 3;
    h.compatible_version_minor = 2;
    h.protocol_version         = 1;
    h.fingerprint              = 0x0102030405060708;
    h.key_id.fill(std::byte{0x22});
    h.own_nonce.fill(std::byte{0x33});
    h.cipher_offer = 1;
    return h;
}

wire::handshake_response handshake_response(wire::handshake_status status)
{
    auto response   = handshake_fields<wire::handshake_response>();
    response.status = status;
    return response;
}

wire::subscribe_request subscribe_request(bool typed)
{
    return {.fqn           = "/plexus/chatter",
            .type_name     = typed ? "std_msgs/String" : "",
            .type_declared = typed,
            .topic_hash    = k_topic,
            .type_hash     = 9,
            .source        = wire::endpoint_source_type::publisher,
            .has_qos       = typed,
            .qos           = {.durability                  = 1,
                              .delivery_mode               = 0,
                              .replay_depth                = 4,
                              .requested_flags             = 1,
                              .requested_deadline_ns       = 0,
                              .requested_lease_ns          = 0,
                              .requested_priority          = 0,
                              .requested_max_message_bytes = 0}};
}

wire::topic_declaration declaration(wire::type_state state)
{
    return {.topic_hash = k_topic, .type_id = 5, .fqn = "/plexus/chatter", .type_name = state == wire::type_state::declared ? "std_msgs/String" : "", .state = state};
}

}

// Grammar: bit 0 of the first byte picks the decoder and bit 1 the source-identity counter, so each unidirectional
// seed's source byte carries its own selection; a bidirectional seed rides behind an odd selector.
seed_set data_frame_seeds()
{
    const wire::unidirectional_header plain{.source = wire::endpoint_source_type::plexus, .sequence = 1, .topic_hash = k_topic};
    const wire::unidirectional_header counted{.source = wire::endpoint_source_type::caller, .sequence = 2, .topic_hash = k_topic};
    seed framed;
    wire::encode_unidirectional_frame_into(framed, {.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0}, plain, text("payload"));
    return {wire::encode_unidirectional(plain, text("hello")), wire::encode_unidirectional(counted, text("hello"), 300), prefixed({1}, wire::encode_bidirectional(k_call, text("call"))),
            framed};
}

seed_set rpc_frame_seeds()
{
    return {wire::encode_rpc_request(k_call, text("args")), wire::encode_rpc_response(k_call, wire::rpc_status::success, text("result")),
            wire::encode_rpc_response(k_call, wire::rpc_status::no_handler, {})};
}

seed_set handshake_frame_seeds()
{
    return {wire::encode_handshake_request(handshake_fields<wire::handshake_request>()), wire::encode_handshake_response(handshake_response(wire::handshake_status::accepted)),
            wire::encode_handshake_response(handshake_response(wire::handshake_status::unauthorized))};
}

seed_set subscribe_frame_seeds()
{
    const wire::subscribe_response degraded{
            .topic_hash = k_topic, .status = wire::subscribe_status::subscribed_degraded, .offer = wire::offer_state::offered, .has_degraded = true, .degraded_flags = 2};
    return {wire::encode_subscribe_request(subscribe_request(false)), wire::encode_subscribe_request(subscribe_request(true)),
            wire::encode_subscribe_response({.topic_hash = k_topic, .status = wire::subscribe_status::subscribed}), wire::encode_subscribe_response(degraded)};
}

seed_set fetch_latched_frame_seeds()
{
    return {wire::encode_fetch_latched_request({.topic_hash = k_topic, .max_samples = 1}), wire::encode_fetch_latched_request({.topic_hash = k_topic, .max_samples = 0xffffffff})};
}

seed_set heartbeat_frame_seeds()
{
    return {wire::encode_heartbeat({}), wire::encode_heartbeat({.version = 1, .reserved = wire::k_heartbeat_relay_decline_flag})};
}

seed_set topic_declaration_frame_seeds()
{
    return {wire::encode_topic_declaration(declaration(wire::type_state::declared)), wire::encode_topic_declaration(declaration(wire::type_state::declared_empty)),
            wire::encode_topic_declaration(declaration(wire::type_state::undeclared))};
}

// Grammar: the first byte modulo three picks the companion verb, the rest is its payload.
seed_set companion_frame_seeds()
{
    return {prefixed({0}, wire::encode_companion({.topic_hash = k_topic})), prefixed({1}, wire::encode_companion_ready({.topic_hash = k_topic, .reader = 3, .generation = 7})),
            prefixed({2}, wire::encode_companion_switch({.topic_hash = k_topic, .first_sequence = 42}))};
}

seed_set announcement_frame_seeds()
{
    wire::announcement ann{.flags = 0, .universe = 7, .node_id = k_origin, .ttl_secs = 30, .listens = {{"tcp", 7447}, {"udp", 7448}}, .universe_pattern = ""};
    const seed direct    = wire::encode_announcement(ann);
    ann.flags            = wire::k_announcement_goodbye_flag | wire::k_announcement_universe_pattern_flag;
    ann.universe_pattern = "lab/**";
    return {direct, wire::encode_announcement(ann)};
}

seed_set peer_report_frame_seeds()
{
    wire::peer_report report{.origin = k_origin, .origin_universe = 7, .hop = 1, .seq = 2, .flags = wire::k_peer_report_consent_flag, .origin_universe_pattern = "", .topics = {}};
    const seed bare = wire::encode_peer_report(report);
    report.flags |= wire::k_peer_report_universe_pattern_flag | wire::k_peer_report_topics_flag;
    report.origin_universe_pattern = "lab/*";
    report.topics                  = {declaration(wire::type_state::declared), declaration(wire::type_state::undeclared)};
    return {bare, wire::encode_peer_report(report)};
}

seed_set forwarded_frame_seeds()
{
    const wire::frame_header inner{.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0};
    wire::forwarded_frame forwarded{.origin = k_origin, .destination = k_destination, .hop = 1, .seq = 3, .flags = wire::k_forwarded_relay_consent_flag, .inner = {}};
    const seed empty = wire::encode_forwarded_frame(forwarded);
    forwarded.inner  = wire::encode_frame(inner, wire::encode_unidirectional({.source = wire::endpoint_source_type::publisher, .sequence = 1, .topic_hash = k_topic}, text("relayed")));
    return {empty, wire::encode_forwarded_frame(forwarded)};
}
