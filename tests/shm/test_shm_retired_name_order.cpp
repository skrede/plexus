#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/retire_steps.h"
#include "support/sidecar_probe.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/native/detail/posix_slot_lock.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>

#include <sys/mman.h>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using plexus::testing::canonical_of;
using plexus::testing::sidecar_inode;
using plexus::testing::named_identity;
using plexus::testing::k_sidecar_host;
using plexus::testing::sidecar_present;
using plexus::testing::native_broker;
using values = std::vector<std::uint32_t>;

// The slice of a registry entry that ring_lifetime's retired-name removal touches.
struct remover
{
    native_broker::region_handle control;
    native_broker::region_handle slab;
    bool creator = false;
};

// The rest of the retirer's close once its control name is gone, in the order every remover keeps: the sidecar lock file
// while its lock is still held, then the release.
void finish_retire(ring_view &retirer, const std::string &canonical)
{
    plexus::native::detail::unlink_sidecar(canonical);
    retirer.control.release_all_slots();
    retirer.unmap();
}

}

TEST_CASE("retired_name_identity a newcomer between a retirer's name removal and its sidecar removal is refused on a sidecar host, and the next acquire engages",
          "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view retirer{lifetime_fqn("retired_order")};
    const std::string canonical = canonical_of(retirer.name);
    plexus::testing::leave_retire_to(retirer, bus);
    plexus::testing::retire_holding(retirer);
    REQUIRE(::shm_unlink(canonical.c_str()) == 0);
    const std::uint64_t guard = sidecar_inode(canonical);
    {
        ring_holder newcomer{bus, retirer.fqn};
        CHECK(newcomer.acquire(ring_role::producer) == (k_sidecar_host ? acquire_result::failed : acquire_result::created));
        CHECK(newcomer.last_bound() == (k_sidecar_host ? acquire_bound::creator_alive : acquire_bound::none));
        CHECK(sidecar_inode(canonical) == guard);
        newcomer.release(ring_role::producer);
    }
    finish_retire(retirer, canonical);

    ring_holder publisher{bus, retirer.fqn};
    ring_holder reader{bus, retirer.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(5) == loan_status::ok);
    CHECK(reader.received() == values{5});
    CHECK(named_identity(retirer) == publisher.bound_identity());
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
    CHECK_FALSE(retirer.named());
    CHECK_FALSE(sidecar_present(canonical));
}

TEST_CASE("retired_name_identity a remover that opened the sidecar before the retirer removed it locks the one the path names and removes nothing", "[shm][retired_name_identity]")
{
    const bool successor = GENERATE(false, true);
    wake_bus bus;
    ring_view retirer{lifetime_fqn("retired_reopen")};
    const std::string canonical = canonical_of(retirer.name);
    plexus::testing::leave_retire_to(retirer, bus);
    plexus::testing::retire_holding(retirer);
    native_broker broker;
    std::optional<remover> late{std::in_place};
    REQUIRE(broker.attach(retirer.name, late->control) == plexus::shm::region_status::ok);
    REQUIRE(late->control.slot_held_elsewhere(plexus::shm::k_creator_slot));

    REQUIRE(::shm_unlink(canonical.c_str()) == 0);
    finish_retire(retirer, canonical);
    ring_holder publisher{bus, retirer.fqn};
    if(successor)
        REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    CHECK_FALSE(plexus::shm::detail::ring_lifetime::remove_retired(broker, *late, retirer.name));
    CHECK(retirer.named() == successor);
    if(successor)
        CHECK(named_identity(retirer) == publisher.bound_identity());
    late.reset();
    CHECK(sidecar_present(canonical) == (successor && k_sidecar_host));
    publisher.release(ring_role::producer);
    CHECK_FALSE(retirer.named());
    CHECK_FALSE(sidecar_present(canonical));
}
