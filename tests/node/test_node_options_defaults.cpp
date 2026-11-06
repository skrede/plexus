#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/detail/node_options_resolve.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>

namespace {

using namespace std::chrono_literals;

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::detail::resolved_redial_seed;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct host
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};
    plexus::discovery::static_discovery disc{{}};
};

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

void check_documented_defaults(const plexus::io::reconnect_config &cfg)
{
    CHECK(cfg.min_delay == 100ms);
    CHECK(cfg.max_delay == 10000ms);
    CHECK_FALSE(cfg.max_attempts.has_value());
    CHECK_FALSE(cfg.max_elapsed.has_value());
}

void require_refused(std::chrono::milliseconds min_delay, std::chrono::milliseconds max_delay)
{
    host h;
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{min_delay, max_delay, std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    const auto build = [&] { inproc_node n{h.ex, h.disc, make_id(0x51), h.transport, opts}; };
    REQUIRE_THROWS_AS(build(), std::runtime_error);
}

}

TEST_CASE("node_options: a value-initialized and a designated-initialized options object yield the documented reconnect defaults", "[node][options]")
{
    check_documented_defaults(plexus::node_options{}.reconnect);
    check_documented_defaults(plexus::node_options{.handshake_timeout = 2s}.reconnect);
}

TEST_CASE("node_options: a default-constructed options object yields the documented reconnect defaults", "[node][options]")
{
    plexus::node_options opts;
    check_documented_defaults(opts.reconnect);
}

TEST_CASE("node: a reconnect config with min_delay zero is refused at construction", "[node][options]")
{
    require_refused(0ms, 1000ms);
}

TEST_CASE("node: a reconnect config with a negative min_delay is refused at construction", "[node][options]")
{
    require_refused(-5ms, 1000ms);
}

TEST_CASE("node: a reconnect config with max_delay below min_delay is refused at construction", "[node][options]")
{
    require_refused(500ms, 100ms);
}

TEST_CASE("node: a node built from default options constructs", "[node][options]")
{
    host h;
    inproc_node n{h.ex, h.disc, make_id(0x52), h.transport, plexus::node_options{}};

    REQUIRE(n.id() == make_id(0x52));
}

TEST_CASE("node_options: an empty redial seed resolves from the node id and a set seed is used as given", "[node][options]")
{
    const plexus::node_options unset{};
    REQUIRE(resolved_redial_seed(make_id(0x01), unset) != resolved_redial_seed(make_id(0x02), unset));
    REQUIRE(resolved_redial_seed(make_id(0x01), unset) == resolved_redial_seed(make_id(0x01), unset));

    plexus::node_options zero;
    zero.redial_seed = 0u;
    REQUIRE(resolved_redial_seed(make_id(0x01), zero) == 0u);

    plexus::node_options set;
    set.redial_seed = 0xC0FFEEu;
    REQUIRE(resolved_redial_seed(make_id(0x01), set) == 0xC0FFEEu);
}
