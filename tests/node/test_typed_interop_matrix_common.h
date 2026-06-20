// The typed↔bytes interop matrix: every boundary between a typed and a bytes endpoint
// (and a typed pair declaring incompatible identities) has a behavioral cell here, each
// asserting the OBSERVABLE outcome — never merely the absence of a crash. The cells split
// across this file and test_typed_fastpath.cpp; together they cover the full matrix.
// The codec is a hand-rolled trivial struct codec — plexus never names a serializer.
#pragma once

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
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

namespace typed_interop_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node      = plexus::node<inproc_policy, inproc_transport<>>;
using bytes_publisher  = plexus::publisher<>;
using bytes_subscriber = plexus::subscriber<>;

struct sample
{
    std::uint32_t value{};
};

// A counting codec with an explicit wire type identity. The encode counter is shared so
// the test body observes a publisher's encode activity regardless of the codec copy the
// endpoint holds.
struct counting_codec
{
    using value_type = sample;

    std::uint64_t                     tag     = 0xABCD1234u;
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

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes,
                                                   sample                    &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                    plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {tag, "sample"}; }
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
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0x511CEu;
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

// Single-dialer topology (subscriber node A eager, publisher node B lazy), copied from the
// bytes fixture — both-eager double-delivers on a shared bus.
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

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

} // namespace typed_interop_fixture
