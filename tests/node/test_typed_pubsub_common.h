// Typed pub/sub over the node facade, two nodes on a shared inproc bus + static_discovery
// (single-dialer, copied from the bytes fixture). It proves the byte fallback round-trip,
// the zero-serialization fast path (delivery by address, zero encodes), the publish(const
// T&) convenience form, the decode-failure default + opt-in escape, the strict/lenient
// posture, and atomic retire. The codec is a hand-rolled trivial struct codec defined here
// — plexus never names a serializer library.
#pragma once

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace typed_pubsub_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node     = plexus::node<inproc_policy, inproc_transport<>>;
using bytes_publisher = plexus::publisher<>;

// A trivial value type: one 32-bit number. The codec serializes it little-endian and
// counts each encode so the fast-path zero-serialization witness is observable.
struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    // The encode counter is shared so a publisher's encode activity is observable from the
    // test body regardless of which codec copy the endpoint holds.
    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
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
        return {0xABCD1234u, "sample"};
    }
};

static_assert(plexus::typed_codec<counting_codec>);
static_assert(plexus::identity_bearing<counting_codec>);

using typed_publisher  = plexus::publisher<counting_codec>;
using typed_subscriber = plexus::subscriber<counting_codec>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0x7A9EDu;
    opts.dial_eagerly = eager;
    return opts;
}

inline std::span<const std::byte> as_bytes(const std::vector<std::byte> &b)
{
    return {b.data(), b.size()};
}

inline std::vector<std::byte> encode_u32(std::uint32_t v)
{
    std::vector<std::byte> b(4);
    for(int i = 0; i < 4; ++i)
        b[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
    return b;
}

// Only the subscriber node (A) dials eagerly; the publisher node (B) stays lazy (the
// single-dialer topology, copied from the bytes fixture).
struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive()
    {
        ex.drain();
    }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

} // namespace typed_pubsub_fixture
