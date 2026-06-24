// publish_object: the zero-serialization sibling of publish. A same-process
// subscriber whose stored type_id matches the carrier's wire tag receives the object
// handle through the inproc lane with the encode callback never invoked; every
// ineligible subscriber (bytes family, tag mismatch, no object lane) takes the byte
// path with encode invoked AT MOST ONCE per publish; a latched topic forces exactly
// one encode even when every live subscriber fast-paths; the caller's slot reference
// is balanced on every path.
#pragma once

#include "plexus/io/message_forwarder.h"
#include "plexus/io/object_carrier.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace publish_object_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::io::loan_slot;
using plexus::io::object_carrier;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

struct counted_payload
{
    std::string value;
    int         release_calls{0};
    loan_slot   slot{};
};

inline object_carrier make_carrier(counted_payload &p, std::uint64_t tag)
{
    p.slot.object  = &p.value;
    p.slot.refs    = 1; // the caller owns one reference on entry to publish_object
    p.slot.release = [](loan_slot *s)
    {
        auto *owner = reinterpret_cast<counted_payload *>(reinterpret_cast<std::byte *>(s) - offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{0, tag, &p.value, 0, 0, &p.slot};
}

// A capture sink recording both the byte frames and the object carriers the forwarder
// fans toward a peer's channel.
struct sink_peer
{
    explicit sink_peer(inproc_executor<> &ex, std::string node_name)
            : fwd_channel(ex)
            , sink(ex)
            , name(std::move(node_name))
    {
        fwd_channel.connect_to(sink.local_endpoint());
        sink.on_data([this](std::span<const std::byte> d) { byte_frames.emplace_back(d.begin(), d.end()); });
        sink.on_object(
                [this](const object_carrier &c)
                {
                    objects.push_back(c);
                    plexus::io::release(c); // the receiving handler owns the delivered reference
                });
    }

    forwarder::peer peer()
    {
        return forwarder::peer{fwd_channel, name};
    }

    inproc_channel<>                    fwd_channel;
    inproc_channel<>                    sink;
    std::string                         name;
    std::vector<std::vector<std::byte>> byte_frames;
    std::vector<object_carrier>         objects;
};

inline std::size_t count_data_frames(const sink_peer &s)
{
    std::size_t n = 0;
    for(const auto &f : s.byte_frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(hdr && hdr->type == plexus::wire::msg_type::unidirectional)
            ++n;
    }
    return n;
}

constexpr std::uint64_t k_tag = 0x7777;

} // namespace publish_object_fixture
