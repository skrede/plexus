#ifndef HPP_GUARD_TESTS_INTEGRATION_WIRE_PUBLIC_API_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_WIRE_PUBLIC_API_COMMON_H

// The construction-time per-transport wire-capture public-API oracle: using ONLY the public
// node facade + node_options (no internal recording include in the opt-in path), a node
// composed over the wire-capturing policy + transport mints the recording_channel decorator
// at the single channel-mint point, so a recorder attached through node.make_recorder()
// captures wire_frame records off the live session. A node composed over the bare inproc
// policy mints bare channels — the decorator is STRUCTURALLY ABSENT (a compile-time type
// witness proves it), and a recorder on it records no wire_frame. The opt-in is a designated-
// initializer node_options field (consumer-sovereign, RAII), never a setter.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/wire_capture_qos.h"

#include "plexus/io/recording_channel.h"
#include "plexus/io/wire_capturing_transport.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace wire_public_api_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using plexus::io::recording_channel;
using plexus::io::wire_capturing_policy;
using plexus::io::wire_capturing_transport;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;

// The wire-capturing composition over the inproc substrate: the policy's byte_channel_type IS
// the decorator, so the engine minted over it mints recording_channel channels — the wire
// tier is present BY TYPE, fixed at construction.
using wire_policy    = wire_capturing_policy<inproc_policy>;
using wire_transport = wire_capturing_transport<inproc_transport<>, inproc_policy>;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;
using wire_node = plexus::node<wire_policy, wire_transport>;

// Structural-absence witness (compile-time): a default node's channel type is a bare inproc
// channel, NOT a recording_channel; only the wire-capturing node's channel type is the
// decorator. The decorated-vs-bare TYPE is fixed at the node's construction.
static_assert(!plexus::io::is_recording_channel_v<inproc_policy::byte_channel_type>, "a default node's channel must NOT be a recording_channel — structurally absent");
static_assert(plexus::io::is_recording_channel_v<wire_policy::byte_channel_type>, "the wire-capturing node's channel must be a recording_channel — structurally present");

struct reading
{
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, reading &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x9A9A0001u, "reading"};
    }
};

static_assert(plexus::typed_codec<reading_codec>);

using typed_publisher  = plexus::publisher<reading_codec>;
using typed_subscriber = plexus::subscriber<reading_codec>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options base_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = eager;
    return opts;
}

inline std::size_t count_wire_frames(std::span<const std::byte> stream)
{
    record_stream_reader r{stream};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    REQUIRE(r.recover(out).header_ok);
    std::size_t n = 0;
    for(const auto &rec : out)
        if(rec.category == record_category::wire_frame)
            ++n;
    return n;
}

}

#endif
