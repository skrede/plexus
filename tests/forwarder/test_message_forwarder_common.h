#pragma once

#include "plexus/io/message_forwarder.h"
#include "plexus/io/wire_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace message_forwarder_fixture {

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

// Maintainability gate: the forwarder models the wire_forwarder shape.
static_assert(plexus::io::wire_forwarder<forwarder, forwarder::peer>);

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A capture sink: a second inproc channel paired to the channel the forwarder
// sends over, recording every delivered frame so the test can decode what the
// forwarder emitted (subscribe / unsubscribe / data) after a drain().
struct capture
{
    explicit capture(inproc_executor<> &ex)
            : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d)
                     { frames.emplace_back(d.begin(), d.end()); });
    }

    inproc_channel<>                    sink;
    std::vector<std::vector<std::byte>> frames;
};

// Wires fwd_channel -> cap.sink so a forwarder send() on fwd_channel surfaces in
// cap.frames after drain(). Returns a peer keyed on node_name.
inline forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Counts frame_header-wrapped subscribe_request frames in a capture's recorded
// traffic. Control frames are framed, so the helper FIRST strips and asserts the
// frame_header.type, THEN decodes the inner payload.
inline std::size_t count_subscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_subscribe_request(inner))
            ++n;
    }
    return n;
}

inline std::size_t count_unsubscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::unsubscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_unsubscribe_request(inner))
            ++n;
    }
    return n;
}

// Decode the type_hash of the FIRST subscribe_request in a capture's recorded traffic.
inline std::optional<std::uint64_t> first_subscribe_type_hash(const capture &cap)
{
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(auto req = plexus::wire::decode_subscribe_request(inner))
            return req->type_hash;
    }
    return std::nullopt;
}

// A test logger whose warn() bumps a counter — proves the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    void        warn(std::string_view) override { ++count; }
    std::size_t count{0};
};

// Builds a valid framed unidirectional message for the named fqn carrying body.
inline std::vector<std::byte> make_data_frame(std::string_view fqn, const std::string &body)
{
    plexus::wire::unidirectional_header uhdr{.source =
                                                     plexus::wire::endpoint_source_type::publisher,
                                             .sequence   = 0,
                                             .topic_hash = plexus::wire::fqn_topic_hash(fqn)};
    auto                       inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));
    plexus::wire::frame_header fhdr{.type         = plexus::wire::msg_type::unidirectional,
                                    .flags        = 0,
                                    .session_id   = 0,
                                    .timestamp_ns = 0,
                                    .payload_len  = inner.size()};
    return plexus::wire::encode_frame(fhdr, inner);
}

// Resurrect every remembered demand through the COUNTED attach path the session's
// reconnect uses (peer_session::resubscribe_all reads remembered_topics and re-attaches
// each demand carrying its stored qos + type_id). A forwarder with no live refcount for
// a pair re-attaches as a 0->1 transition, emitting exactly one subscribe.
inline void resurrect(forwarder &fwd, const forwarder::peer &peer)
{
    for(const auto &demand : fwd.remembered_topics(peer.node_name))
        fwd.attach(peer, demand.fqn, demand.qos, demand.type_id);
}

} // namespace message_forwarder_fixture
