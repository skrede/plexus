#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/region_names.h"
#include "support/sidecar_probe.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <thread>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::native_broker;

constexpr std::uint32_t k_rounds = 10000;

struct tally
{
    std::uint32_t acquired   = 0;
    std::uint32_t mismatched = 0;
};

bool names_held_ring(native_broker &broker, const std::string &name, const ring_holder &holder)
{
    native_broker::region_handle fresh;
    return broker.attach(name, fresh) == plexus::shm::region_status::ok && plexus::shm::detail::ring_lifetime::identity_of(fresh.bytes()) == holder.bound_identity();
}

// One registry of its own in one role, retiring the ring whenever its release leaves no holder, while the other registry
// does the same in the other role.
tally churn(const std::string &fqn, const std::string &name, ring_role role)
{
    wake_bus bus;
    ring_holder holder{bus, fqn};
    native_broker broker;
    tally counted;
    for(std::uint32_t round = 0; round < k_rounds; ++round)
    {
        if(holder.acquire(role) == acquire_result::failed)
            continue;
        ++counted.acquired;
        if(!names_held_ring(broker, name, holder))
            ++counted.mismatched;
        holder.release(role);
    }
    return counted;
}

}

// Probabilistic: the interleavings are the scheduler's, so a pass shows only that none of these rounds broke the rule. One
// producer, since two producers minting one name concurrently race in the mint itself, outside this rule.
TEST_CASE("retired_name_identity a publisher and a reader retiring and acquiring one name always find it naming the ring they hold", "[shm][retired_name_identity][probabilistic]")
{
    ring_view view{plexus::testing::lifetime_fqn("retired_churn")};
    std::array<tally, 2> counted{};
    std::thread first([&] { counted[0] = churn(view.fqn, view.name, ring_role::producer); });
    std::thread second([&] { counted[1] = churn(view.fqn, view.name, ring_role::consumer); });
    first.join();
    second.join();

    CHECK(counted[0].acquired > 0);
    CHECK(counted[1].acquired > 0);
    CHECK(counted[0].mismatched == 0);
    CHECK(counted[1].mismatched == 0);
    CHECK_FALSE(view.named());
    CHECK_FALSE(plexus::testing::sidecar_present(plexus::testing::canonical_of(view.name)));
}
