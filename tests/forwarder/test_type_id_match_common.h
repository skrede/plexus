// The subscribe-time type_id match: a producer reacting to an arriving subscribe
// (attach_for_fanout) compares the subscriber's declared type_id against its own
// declared producer type_id. A match (or either side undeclared) stays subscribed;
// a real mismatch is refused with subscribe_status::type_mismatch and NO fan-out
// entry is registered. The type_id rides the already-on-wire subscribe_request
// type_hash field (0 = undeclared); matching authority is subscribe-time discovery.
#pragma once

#include "plexus/io/message_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace type_id_match_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

// A capture sink recording every frame the forwarder sends, so the test can decode
// the subscribe_response the producer emitted.
struct capture
{
    explicit capture(inproc_executor<> &ex)
            : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d) { frames.emplace_back(d.begin(), d.end()); });
    }

    inproc_channel<>                    sink;
    std::vector<std::vector<std::byte>> frames;
};

inline forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Decode the FIRST subscribe_response status in a capture's recorded traffic.
inline std::optional<plexus::wire::subscribe_status> first_response_status(const capture &cap)
{
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe_response)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(auto resp = plexus::wire::decode_subscribe_response(inner))
            return resp->status;
    }
    return std::nullopt;
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::size_t count_data_frames(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(hdr && hdr->type == plexus::wire::msg_type::unidirectional)
            ++n;
    }
    return n;
}

inline plexus::io::subscriber_qos strict_typed()
{
    plexus::io::subscriber_qos q;
    q.posture = plexus::io::attach_posture::strict;
    return q;
}

} // namespace type_id_match_fixture
