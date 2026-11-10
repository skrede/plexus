#include "support/freertos_loopback_policy.h"

#include "plexus/post.h"
#include "plexus/node_id.h"
#include "plexus/topic_query.h"
#include "plexus/loopback_node.h"
#include "plexus/participant_query.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/freertos/freertos_executor.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <catch2/matchers/catch_matchers.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace {

using plexus::testing::freertos_loopback_policy;

using freertos_host = plexus::loopback_host<freertos_loopback_policy>;

constexpr std::uint32_t k_pool = plexus::freertos::freertos_executor::k_queue_depth;

plexus::node_id make_id()
{
    plexus::node_id id{};
    id[0] = std::byte{0x04};
    return id;
}

// Every ingress slot holds a post nobody has pumped yet, so the next cross-thread post is refused.
struct full_pool_rig
{
    full_pool_rig()
            : ex()
            , disc({})
            , host(ex, disc, make_id())
    {
        for(std::uint32_t i = 0; i < k_pool; ++i)
            REQUIRE(ex.post_from_thread([] {}));
    }

    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc;
    freertos_host host;
};

struct no_argument_work
{
    void operator()() const noexcept
    {
    }
};

template<typename T>
constexpr bool posts_to = requires(T &target, no_argument_work fn) { plexus::post(target, fn); };

}

TEST_CASE("post_refused participants_blocking fails closed when the executor refuses the post", "[node]")
{
    full_pool_rig rig;
    std::array<plexus::graph::participant_record, 4> seen{};

    REQUIRE_THROWS_WITH(plexus::participants_blocking(rig.host.node(), std::span<plexus::graph::participant_record>{seen}),
                        "plexus: participants_blocking could not reach the node's executor");
    REQUIRE(rig.ex.ingress_dropped() == 1);
    rig.ex.drain();
}

TEST_CASE("post_refused topics_blocking fails closed when the executor refuses the post", "[node]")
{
    full_pool_rig rig;
    std::array<plexus::graph::topic_record, 4> seen{};

    REQUIRE_THROWS_WITH(plexus::topics_blocking(rig.host.node(), std::span<plexus::graph::topic_record>{seen}), "plexus: topics_blocking could not reach the node's executor");
    REQUIRE(rig.ex.ingress_dropped() == 1);
    rig.ex.drain();
}

TEST_CASE("post_refused plexus::post names only a node", "[node]")
{
    CHECK(posts_to<plexus::loopback_node<freertos_loopback_policy>>);
    CHECK_FALSE(posts_to<std::int32_t>);
}

TEST_CASE("post_refused plexus::post reports a refusal on a full FreeRTOS executor", "[node]")
{
    full_pool_rig rig;
    bool refused_ran = false;

    CHECK_FALSE(plexus::post(rig.host.node(), [&refused_ran] { refused_ran = true; }));
    CHECK(rig.ex.ingress_dropped() == 1);
    rig.ex.drain();
    CHECK_FALSE(refused_ran);
}
