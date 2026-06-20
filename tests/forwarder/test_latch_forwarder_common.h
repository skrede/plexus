#pragma once

#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/topic_qos.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace latch_forwarder_fixture {

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A capture sink: a second inproc channel paired to the channel the forwarder
// sends over, recording every delivered frame so the test can decode what the
// forwarder emitted after a drain().
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

inline forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Collects the bodies of the UNIDIRECTIONAL data frames in a capture: strip the
// frame_header, assert the type is unidirectional, decode the inner, return the
// opaque payload as a string. A frame whose header is not unidirectional (e.g. a
// subscribe_response control frame) is skipped.
inline std::vector<std::string> data_bodies(const capture &cap)
{
    std::vector<std::string> bodies;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::unidirectional)
            continue;
        auto inner   = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        auto decoded = plexus::wire::decode_unidirectional(inner);
        if(!decoded)
            continue;
        bodies.emplace_back(reinterpret_cast<const char *>(decoded->data.data()),
                            decoded->data.size());
    }
    return bodies;
}

} // namespace latch_forwarder_fixture
