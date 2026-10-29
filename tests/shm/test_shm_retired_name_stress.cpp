#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/region_names.h"
#include "support/sidecar_probe.h"

#include "plexus/shm/detail/shm_ring_names.h"

#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <string>
#include <thread>
#include <cstddef>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::native_broker;

constexpr std::uint32_t k_rounds    = 10000;
constexpr std::uint32_t k_round_cap = 10 * k_rounds;

struct tally
{
    std::uint32_t acquired   = 0;
    std::uint32_t mismatched = 0;
};

bool names_held_ring(native_broker &broker, const std::string &name, const ring_holder &holder)
{
    native_broker::region_handle fresh;
    return broker.attach(name, fresh) == plexus::shm::region_status::ok && plexus::shm::detail::ring_names::identity_of(fresh.bytes()) == holder.bound_identity();
}

// One registry of its own in one role, retiring the ring whenever its release leaves no holder, while the other registry
// does the same in the other role. A reader acquires only while a publisher's ring exists, so every thread churns past
// k_rounds until each has acquired once, and the cap turns a thread that never acquires into a failure, not a hang.
tally churn(const std::string &fqn, const std::string &name, ring_role role, std::atomic<std::uint32_t> &unacquired)
{
    wake_bus bus;
    ring_holder holder{bus, fqn};
    native_broker broker;
    tally counted;
    for(std::uint32_t round = 0; round < k_round_cap && (round < k_rounds || unacquired.load() > 0); ++round)
    {
        if(holder.acquire(role) == acquire_result::failed)
            continue;
        if(++counted.acquired == 1)
            unacquired.fetch_sub(1);
        if(!names_held_ring(broker, name, holder))
            ++counted.mismatched;
        holder.release(role);
    }
    return counted;
}

template<std::size_t N>
std::array<tally, N> churn_together(const ring_view &view, const std::array<ring_role, N> &roles)
{
    std::array<tally, N> counted{};
    std::array<std::thread, N> threads;
    std::atomic<std::uint32_t> unacquired{N};
    for(std::size_t i = 0; i < N; ++i)
        threads[i] = std::thread([&counted, &view, &roles, &unacquired, i] { counted[i] = churn(view.fqn, view.name, roles[i], unacquired); });
    for(std::thread &thread : threads)
        thread.join();
    return counted;
}

}

// Probabilistic: the interleavings are the scheduler's, so a pass shows only that none of these rounds broke the rule.
TEST_CASE("retired_name_identity a publisher and a reader retiring and acquiring one name always find it naming the ring they hold", "[shm][retired_name_identity][probabilistic]")
{
    ring_view view{plexus::testing::lifetime_fqn("retired_churn")};
    const std::array<tally, 2> counted = churn_together(view, std::array{ring_role::producer, ring_role::consumer});

    CHECK(counted[0].acquired > 0);
    CHECK(counted[1].acquired > 0);
    CHECK(counted[0].mismatched == 0);
    CHECK(counted[1].mismatched == 0);
    CHECK_FALSE(view.named());
    CHECK_FALSE(plexus::testing::sidecar_present(plexus::testing::canonical_of(view.name)));
}

TEST_CASE("retired_name_identity two publishers and a reader retiring and acquiring one name always find it naming the ring they hold", "[shm][retired_name_identity][probabilistic]")
{
    ring_view view{plexus::testing::lifetime_fqn("retired_churn_two")};
    const std::array<tally, 3> counted = churn_together(view, std::array{ring_role::producer, ring_role::producer, ring_role::consumer});

    CHECK(counted[0].acquired > 0);
    CHECK(counted[1].acquired > 0);
    CHECK(counted[2].acquired > 0);
    for(const tally &one : counted)
        CHECK(one.mismatched == 0);
    CHECK_FALSE(view.named());
    CHECK_FALSE(plexus::testing::sidecar_present(plexus::testing::canonical_of(view.name)));
}
