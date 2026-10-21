#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_COMMON_H

#include "test_node_close_transports.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/intra_node_transport.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>

namespace node_close_fixture {

using plexus::io::endpoint;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

class counting_discovery final : public plexus::discovery::discovery
{
public:
    counting_discovery()
            : m_stops(0)
            , m_inner({})
    {
    }

    void advertise(const plexus::discovery::service_info &service) override
    {
        m_inner.advertise(service);
    }

    void browse(const resolved_callback &on_resolved) override
    {
        m_inner.browse(on_resolved);
    }

    void stop() override
    {
        ++m_stops;
        m_inner.stop();
    }

    std::int32_t stops() const noexcept
    {
        return m_stops;
    }

private:
    std::int32_t m_stops;
    plexus::discovery::static_discovery m_inner;
};

using dialer_node   = plexus::node<inproc_policy, deferred_dial_transport>;
using listener_node = plexus::node<inproc_policy, inproc_transport<>>;
using composed_node = plexus::node<inproc_policy, plexus::io::intra_node_transport<inproc_policy>, recording_member>;

inline constexpr std::chrono::milliseconds k_backoff{2};

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{k_backoff, k_backoff * 2, std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

template<typename Node>
void close_or_fail(Node &node)
{
    if constexpr(requires { node.close(); })
        node.close();
    else
        FAIL("the node has no close()");
}

// A dials B's listen endpoint; neither discovers the other, so the dialed slot is the only route between them.
struct close_net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    deferred_dial_transport ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    counting_discovery disc_a;
    counting_discovery disc_b;
    endpoint ep_a{"inproc", "host-a:5000"};
    endpoint ep_b{"inproc", "host-b:6000"};
    plexus::node_id dialed_slot{plexus::io::endpoint_id(ep_b)};
    dialer_node a{ex, disc_a, make_id(0x0A), ta, make_opts()};
    listener_node b{ex, disc_b, make_id(0x0B), tb, make_opts()};

    close_net()
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void connect()
    {
        a.dial(ep_b);
        ex.drain();
        REQUIRE(a.router().is_connected(dialed_slot));
        REQUIRE(b_sessions() == 1);
    }

    std::size_t b_sessions()
    {
        std::size_t n = 0;
        b.router().registry().for_each_connected([&n](const plexus::node_id &, auto &) { ++n; });
        return n;
    }

    std::size_t live_channels() const
    {
        std::size_t n = 0;
        for(std::uint32_t key = 0; key < 256; ++key)
            n += bus.key_for({"inproc", std::to_string(key)}) != 0 ? 1 : 0;
        return n;
    }

    // The key the bus mints next: two calls one apart mean no channel was minted between them.
    std::uint64_t mint_key()
    {
        inproc_channel<> probe{ex};
        return std::stoull(probe.local_endpoint().address);
    }

    void run_for(std::chrono::milliseconds span)
    {
        const auto until = std::chrono::steady_clock::now() + span;
        while(std::chrono::steady_clock::now() < until)
            ex.step();
    }
};

}

#endif
