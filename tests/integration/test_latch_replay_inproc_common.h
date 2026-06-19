#ifndef HPP_GUARD_TESTS_INTEGRATION_LATCH_REPLAY_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_LATCH_REPLAY_INPROC_COMMON_H

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/data_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <system_error>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace latch_replay_fixture {

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A subscriber-side receive sink: an inproc channel the publisher fans toward,
// whose received frames route through a REAL frame_router. on_unidirectional
// decodes the inner unidirectional payload (the router owns the frame_header strip
// + the type switch — the production receive contract, not a hand-strip) and
// records the opaque bytes. This is the headline assertion surface: a late joiner
// recovers the retained value through the same demux any live frame traverses.
struct receive_sink
{
    explicit receive_sink(inproc_executor<> &ex)
            : channel(ex)
    {
        router.on_unidirectional(
                [this](const plexus::wire::frame_header &, std::span<const std::byte> inner)
                {
                    if(auto decoded = plexus::wire::decode_unidirectional(inner))
                        bodies.emplace_back(to_string(decoded->data));
                });
        channel.on_data([this](std::span<const std::byte> f) { router.route(f); });
    }

    inproc_channel<>         channel;
    plexus::io::frame_router router;
    std::vector<std::string> bodies;
};

inline forwarder::peer make_peer(inproc_channel<> &fwd_channel, receive_sink &sink,
                                 std::string node_name)
{
    fwd_channel.connect_to(sink.channel.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

}

#endif
