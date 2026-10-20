#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"

#include "plexus/shm/ring_header.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/native/shm_error.h"

#include "plexus/native/detail/posix_slot_lock.h"
#include "plexus/native/detail/posix_shm_status.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <filesystem>

#include <unistd.h>

#include <sys/mman.h>

namespace {

using plexus::shm::ring_role;
using plexus::shm::slot_hold;
using plexus::shm::cursor_slot;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;

// Where a sidecar lock file carries the slots, the newcomer creates a fresh one, wins the creator slot on it and declines
// the retired ring; where the region's own record locks carry them, the retirer's hold refuses it the slot.
#if defined(__APPLE__)
constexpr bool k_sidecar_host              = true;
constexpr acquire_bound k_newcomer_refusal = acquire_bound::transient_join;

bool sidecar_present(const std::string &canonical)
{
    std::array<char, 1024> dir{};
    const std::size_t counted = ::confstr(_CS_DARWIN_USER_TEMP_DIR, dir.data(), dir.size());
    REQUIRE((counted > 1 && counted <= dir.size()));
    return std::filesystem::exists(std::filesystem::path(dir.data()) / (canonical.substr(1) + ".lk"));
}
#else
constexpr bool k_sidecar_host              = false;
constexpr acquire_bound k_newcomer_refusal = acquire_bound::creator_alive;

bool sidecar_present(const std::string &)
{
    return false;
}
#endif

std::string canonical_of(const std::string &name)
{
    std::string canonical;
    REQUIRE(plexus::native::detail::sanitize_region_name(name, canonical) == plexus::native::shm_error::ok);
    return canonical;
}

// The publisher leaves while the retirer holds a reader slot, so the retire falls to the retirer, as to any last holder.
void leave_retire_to(ring_view &retirer, wake_bus &bus)
{
    ring_holder publisher{bus, retirer.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(retirer.map());
    REQUIRE(retirer.control.hold_slot(cursor_slot(0)) == slot_hold::held);
    publisher.release(ring_role::producer);
    retirer.control.release_slot(cursor_slot(0));
}

// ring_lifetime::close up to the middle of the control handle's unlink_name: every slot held, the ring retired, the
// slab's name and the sidecar lock file removed, the control name kept.
void retire_up_to_control_name(ring_view &retirer, const std::string &canonical)
{
    REQUIRE(retirer.control.hold_all_slots() == slot_hold::held);
    std::uint64_t found = plexus::shm::k_ring_magic;
    REQUIRE(retirer.ring.header().magic.compare_exchange_strong(found, plexus::shm::k_ring_retired));
    retirer.slab.unlink_name();
    REQUIRE(sidecar_present(canonical) == k_sidecar_host);
    plexus::native::detail::unlink_sidecar(canonical);
    REQUIRE_FALSE(sidecar_present(canonical));
}

}

TEST_CASE("sidecar_leak a newcomer that maps a ring between its retirer's sidecar removal and control-name removal leaves no sidecar behind", "[shm][sidecar_leak]")
{
    wake_bus bus;
    ring_view retirer{lifetime_fqn("sidecar_leak")};
    const std::string canonical = canonical_of(retirer.name);
    leave_retire_to(retirer, bus);
    retire_up_to_control_name(retirer, canonical);
    {
        ring_holder newcomer{bus, retirer.fqn};
        REQUIRE(newcomer.acquire(ring_role::producer) == acquire_result::failed);
        CHECK(newcomer.last_bound() == k_newcomer_refusal);
    }

    REQUIRE(::shm_unlink(canonical.c_str()) == 0);
    retirer.control.release_all_slots();
    retirer.unmap();
    CHECK_FALSE(sidecar_present(canonical));
}
