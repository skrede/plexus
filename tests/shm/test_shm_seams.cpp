#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/notifier_concept.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <string_view>
#include <utility>

// The two core seams (region_broker + notifier) the shared-memory backend and the
// reactor bridge satisfy. These stub models prove the concepts are SATISFIABLE
// (the surface a backend must present is correct and self-consistent) and that the
// notifier's drain is a move-only callback, with NO POSIX/asio in the core TU.

using namespace plexus::shm;

namespace {

// A minimal in-memory broker stub satisfying region_broker: it carries an
// associated region_handle and presents the create/attach/set_attach_policy surface.
struct stub_region_handle
{
    bool mapped = false;
};

struct stub_broker
{
    using region_handle = stub_region_handle;

    region_status create(std::string_view /*name*/, std::size_t /*bytes*/, const create_options & /*opts*/, region_handle &out)
    {
        out.mapped = true;
        return region_status::ok;
    }

    region_status attach(std::string_view /*name*/, region_handle &out)
    {
        out.mapped = true;
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)> p)
    {
        m_policy = std::move(p);
    }

    plexus::detail::move_only_function<bool(std::string_view)> m_policy;
};

// A minimal notifier stub: signal bumps a counter, arm stores the move-only drain,
// disarm clears it.
struct stub_notifier
{
    void signal()
    {
        ++signals;
    }

    void arm(plexus::detail::move_only_function<void()> drain)
    {
        m_drain = std::move(drain);
    }

    void disarm()
    {
        m_drain = nullptr;
    }

    int signals = 0;
    plexus::detail::move_only_function<void()> m_drain;
};

static_assert(region_broker<stub_broker>, "stub_broker must satisfy region_broker");
static_assert(notifier<stub_notifier>, "stub_notifier must satisfy notifier");

}

TEST_CASE("seams: a stub broker satisfies region_broker and round-trips create/attach", "[shm][seams]")
{
    stub_broker broker;
    stub_region_handle h{};

    REQUIRE(broker.create("name", 4096, create_options{}, h) == region_status::ok);
    REQUIRE(h.mapped);

    stub_region_handle h2{};
    REQUIRE(broker.attach("name", h2) == region_status::ok);
    REQUIRE(h2.mapped);

    bool consulted = false;
    broker.set_attach_policy(
            [&](std::string_view)
            {
                consulted = true;
                return true;
            });
    REQUIRE(broker.m_policy);
    REQUIRE(broker.m_policy("name"));
    REQUIRE(consulted);
}

TEST_CASE("seams: a stub notifier satisfies notifier and carries a move-only drain", "[shm][seams]")
{
    stub_notifier n;

    int drained = 0;
    n.arm([&] { ++drained; });
    REQUIRE(n.m_drain);

    // The drain is move-only: invoking it through the stub runs the callback.
    n.m_drain();
    REQUIRE(drained == 1);

    n.signal();
    REQUIRE(n.signals == 1);

    n.disarm();
    REQUIRE_FALSE(n.m_drain);
}

TEST_CASE("seams: create_options default to owner-only perms and no stale reclaim", "[shm][seams]")
{
    const create_options opts{};
    REQUIRE(opts.perms == 0600);
    REQUIRE_FALSE(opts.unlink_stale_on_create);
}
