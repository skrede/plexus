#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_SELF_LOOPBACK_MIXED_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_SELF_LOOPBACK_MIXED_COMMON_H

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/intra_node_transport.h"
#include "plexus/io/transport_selector.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <atomic>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus_test_mixed {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

// A mux member presenting the remote (network) tier over the deterministic inproc bus: it serves a
// DISTINCT scheme ("tcp") from intra_node's "inproc", so the two compose with no scheme collision.
// It forwards every transport verb to an inner inproc_transport; the bus keys listeners on the full
// endpoint, scheme-agnostic, so a "tcp"-scheme dial rendezvous with a "tcp"-scheme listen.
class remote_bus_member
{
public:
    using channel_type = inproc_channel<>;

    static constexpr std::array<std::string_view, 1> k_schemes{"tcp"};
    static constexpr std::span<const std::string_view> mux_schemes{k_schemes};
    static constexpr plexus::io::transport_kind mux_tier = plexus::io::transport_kind::remote;

    remote_bus_member(inproc_executor<> &exec, inproc_bus<> &bus)
            : m_inner(exec, bus)
    {
    }

    void listen(const plexus::io::endpoint &ep)
    {
        m_inner.listen(ep);
    }
    void dial(const plexus::io::endpoint &ep)
    {
        m_inner.dial(ep);
    }
    void close()
    {
        m_inner.close();
    }

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_inner.on_accepted(std::move(cb));
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const plexus::io::endpoint &)> cb)
    {
        m_inner.on_dialed(std::move(cb));
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)> cb)
    {
        m_inner.on_dial_failed(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_inner.on_error(std::move(cb));
    }

private:
    inproc_transport<> m_inner;
};

// Node A's pack: the intra-node self-route ranked first, then the remote bus member. The engine binds
// polymorphic_byte_channel (multi-transport), so self-delivery is the framed bytes lane (no zero-copy
// self-lane there). Node B is a plain single-transport bus node, the remote publisher.
using mixed_node  = plexus::node<inproc_policy, plexus::io::intra_node_transport<inproc_policy>, remote_bus_member>;
using remote_node = plexus::node<inproc_policy, inproc_transport<>>;

struct sample
{
    std::uint32_t value{};
};

// Counts encode calls: a framed-lane self-delivery MUST encode (the erased channel has no
// send_object), proving the multi-transport node took the bytes lane, not a zero-copy object hop.
struct counting_codec
{
    using value_type = sample;

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
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

// Node A (mixed pack) is the eager dialer + the local subscriber + a local publisher; node B is the
// lazy remote publisher A dials. A single shared bus/executor/discovery keeps the topology
// deterministic and single-process — no sockets, no wall-clock pumps.
struct mixed_net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    plexus::io::intra_node_transport<inproc_policy> a_self;
    remote_bus_member a_remote{ex, bus};
    inproc_transport<> tb{ex, bus};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    mixed_node a{ex, disc, id_a, a_self, a_remote, make_opts(/*eager=*/true)};
    remote_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive()
    {
        ex.drain();
    }

    void connect()
    {
        a.listen({"tcp", "host-a:5000"});
        b.listen({"tcp", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

#endif
