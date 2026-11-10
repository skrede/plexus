#include "test_spine_lifecycle_inproc_common.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/demand_transition.h"

#include "plexus/io/detail/upgrade_hints.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <cstddef>
#include <optional>
#include <string_view>

namespace {

using namespace spine_fixture;

using plexus::io::demand_role;
using plexus::io::dispatch_hint;
using plexus::io::demand_transition;

using handle = std::optional<plexus::subscriber<>>;

constexpr std::string_view k_topic = "hint/topic";
constexpr std::string_view k_peer  = "peer-unreciprocated";

void ignore(std::span<const std::byte>, const plexus::io::message_info &)
{
}

struct hint_pair
{
    pair_fixture fx;
    std::optional<plexus::publisher<>> pub;
    handle local;
    handle remote;

    hint_pair()
            : fx()
            , pub()
            , local()
            , remote()
    {
        pub.emplace(fx.a, k_topic);
        local.emplace(fx.a, k_topic, plexus::io::subscriber_qos{.dispatch = dispatch_hint::frequent}, ignore);
        fx.drive();
    }

    dispatch_hint hint()
    {
        return plexus::io::detail::upgrade_hints::hint_for(fx.a.router().coordinator(), k_topic);
    }

    // A peer this node never subscribed back to, so the local subscriber holds no receive lane toward it.
    void unreciprocated_peer_comes_and_goes()
    {
        fx.a.router().coordinator().on_edge(k_peer, k_topic, demand_transition::up, demand_role::publisher);
        pub.reset();
        fx.drive();
        fx.a.router().coordinator().on_edge(k_peer, k_topic, demand_transition::down, demand_role::publisher);
    }

    bool a_holds_b()
    {
        return fx.a.router().messages().has_remote_demand(plexus::wire::fqn_topic_hash(k_topic), nullptr);
    }
};

}

TEST_CASE("dispatch hint: a remaining local subscriber keeps its hint after the last remote subscriber leaves", "[integration][dispatch_hint]")
{
    hint_pair p;
    REQUIRE(p.hint() == dispatch_hint::frequent);

    p.unreciprocated_peer_comes_and_goes();

    CHECK(p.hint() == dispatch_hint::frequent);
}

TEST_CASE("dispatch hint: a topic with no local endpoint and no lane forgets its hint", "[integration][dispatch_hint]")
{
    hint_pair p;
    p.unreciprocated_peer_comes_and_goes();

    p.local.reset();
    p.fx.drive();

    CHECK(p.hint() == dispatch_hint::none);
}

TEST_CASE("dispatch hint: a connected pair keeps the subscriber's hint after the remote subscriber unsubscribes", "[integration][dispatch_hint]")
{
    hint_pair p;
    p.fx.connect();
    p.remote.emplace(p.fx.b, k_topic, ignore);
    p.fx.drive();
    REQUIRE(p.a_holds_b());

    p.pub.reset();
    p.fx.drive();
    p.remote.reset();
    p.fx.drive();
    REQUIRE_FALSE(p.a_holds_b());

    CHECK(p.hint() == dispatch_hint::frequent);
}
