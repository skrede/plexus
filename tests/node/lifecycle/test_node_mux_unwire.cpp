#include "test_node_close_transports.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/intra_node_transport.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace {

using namespace std::chrono_literals;

using plexus::io::endpoint;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::detail::move_only_function;
using node_close_fixture::recording_member;

class bus_member
{
public:
    using channel_type = inproc_channel<>;

    static constexpr std::array<std::string_view, 1> k_schemes{"tcp"};
    static constexpr std::span<const std::string_view> mux_schemes{k_schemes};
    static constexpr plexus::io::transport_kind mux_tier = plexus::io::transport_kind::remote;

    bus_member(inproc_executor<> &ex, inproc_bus<> &bus)
            : m_inner(ex, bus)
    {
    }

    void listen(const endpoint &ep)
    {
        m_inner.listen(ep);
    }

    void dial(const endpoint &ep)
    {
        m_inner.dial(ep);
    }

    void close()
    {
        m_inner.close();
    }

    void on_accepted(move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_inner.on_accepted(std::move(cb));
    }

    void on_dialed(move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> cb)
    {
        m_inner.on_dialed(std::move(cb));
    }

    void on_dial_failed(move_only_function<void(const endpoint &, plexus::io::io_error)> cb)
    {
        m_inner.on_dial_failed(std::move(cb));
    }

    void on_error(move_only_function<void(plexus::io::io_error)> cb)
    {
        m_inner.on_error(std::move(cb));
    }

    std::size_t retiring_size() const noexcept
    {
        return m_inner.retiring_size();
    }

private:
    inproc_transport<> m_inner;
};

using mux_node = plexus::node<inproc_policy, plexus::io::intra_node_transport<inproc_policy>, bus_member>;

struct host
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    plexus::io::intra_node_transport<inproc_policy> self;
    bus_member remote{ex, bus};
    inproc_transport<> dialer{ex, bus};
    plexus::discovery::static_discovery disc{{}};
    const endpoint ep{"tcp", "mux-member:7000"};

    void require_accept_retired_by_member()
    {
        remote.listen(ep);
        dialer.dial(ep);
        REQUIRE(remote.retiring_size() == 1);
        ex.drain();
        REQUIRE(remote.retiring_size() == 0);
    }
};

plexus::node_options make_opts(std::chrono::milliseconds min_delay, std::chrono::milliseconds max_delay)
{
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{min_delay, max_delay, std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

}

TEST_CASE("node: a two-transport node refused at construction leaves its members accepting into no sink", "[node][lifecycle][mux]")
{
    host h;
    const auto build = [&] { return std::make_unique<mux_node>(h.ex, h.disc, make_id(0x71), h.self, h.remote, make_opts(0ms, 1000ms)); };
    REQUIRE_THROWS_AS(build(), std::runtime_error);

    h.require_accept_retired_by_member();
}

TEST_CASE("node: a two-transport node destroyed without close leaves its members accepting into no sink", "[node][lifecycle][mux]")
{
    host h;
    auto n = std::make_unique<mux_node>(h.ex, h.disc, make_id(0x72), h.self, h.remote, make_opts(50ms, 2000ms));
    h.ex.drain();
    n.reset();

    h.require_accept_retired_by_member();
}

TEST_CASE("multiplexing_transport: a closed mux leaves the sinks a later owner installs on its members", "[node][lifecycle][mux]")
{
    recording_member first;
    recording_member second;
    auto mux = std::make_unique<plexus::io::multiplexing_transport<recording_member, recording_member>>(first, second);
    mux->close();
    first.on_error([](plexus::io::io_error) {});
    mux.reset();

    REQUIRE(first.any_installed());
    REQUIRE_FALSE(second.any_installed());
}
