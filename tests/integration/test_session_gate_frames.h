#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_GATE_FRAMES_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_GATE_FRAMES_H

#include "test_peer_session_inproc_common.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/fetch_latched.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace session_gate_fixture {

using namespace peer_session_inproc_fixture;
namespace wire = plexus::wire;

constexpr std::string_view k_topic     = "gate/topic";
constexpr std::string_view k_latched   = "gate/latched";
constexpr std::string_view k_procedure = "gate/procedure";

inline std::vector<std::byte> frame_of(wire::msg_type type, std::span<const std::byte> payload, std::uint64_t session_id = 0)
{
    const wire::frame_header hdr{.type = type, .flags = 0, .session_id = session_id, .timestamp_ns = 0, .payload_len = payload.size()};
    return wire::encode_frame(hdr, payload);
}

// The last frame of the given type a real forwarder writes onto a scratch channel, so a frame is framed exactly as the
// library frames it.
template<typename Emit>
std::vector<std::byte> captured_frame(wire::msg_type type, Emit emit)
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    plexus::log::null_logger sink;
    msg_forwarder framer{sink};
    inproc_channel<> capture(ex);
    inproc_channel<> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data(
            [&](std::span<const std::byte> f)
            {
                if(const auto hdr = wire::decode_header(f); hdr && hdr->type == type)
                    captured.assign(f.begin(), f.end());
            });
    msg_forwarder::peer peer{tx, "capture"};
    emit(framer, peer);
    ex.drain();
    return captured;
}

inline std::vector<std::byte> data_frame(const std::string &payload, std::uint64_t session_id = 0)
{
    return captured_frame(wire::msg_type::unidirectional,
                          [&](msg_forwarder &f, const msg_forwarder::peer &p)
                          {
                              f.attach_for_fanout(p, k_topic);
                              f.publish(k_topic, as_bytes(payload), session_id);
                          });
}

inline std::vector<std::byte> subscribe_frame()
{
    return captured_frame(wire::msg_type::subscribe, [](msg_forwarder &f, const msg_forwarder::peer &p) { f.attach(p, k_topic); });
}

inline std::vector<std::byte> rpc_request_frame(std::uint64_t session_id)
{
    const wire::bidirectional_header hdr{
            .source = wire::endpoint_source_type::caller, .sequence = 0, .topic_hash = wire::fqn_topic_hash(k_procedure), .type_hash_1 = 0, .type_hash_2 = 0, .correlation_id = 1};
    return frame_of(wire::msg_type::rpc_request, wire::encode_rpc_request(hdr, {}), session_id);
}

// A minimal valid payload for the verbs whose consumer has an observable effect, an empty one for the rest.
inline std::vector<std::byte> frame_for(wire::msg_type type, std::uint64_t session_id = 0)
{
    switch(type)
    {
        case wire::msg_type::unidirectional:
            return data_frame("gate", session_id);
        case wire::msg_type::subscribe:
            return subscribe_frame();
        case wire::msg_type::rpc_request:
            return rpc_request_frame(session_id);
        case wire::msg_type::heartbeat:
            return frame_of(type, wire::encode_heartbeat(wire::heartbeat{}), session_id);
        case wire::msg_type::fetch_latched:
            return frame_of(type, wire::encode_fetch_latched_request({.topic_hash = wire::fqn_topic_hash(k_latched), .max_samples = 1}), session_id);
        default:
            return frame_of(type, {}, session_id);
    }
}

}

#endif
