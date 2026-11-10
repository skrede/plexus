#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/retire_steps.h"
#include "support/sidecar_probe.h"

#include "plexus/shm/shm_acquire_result.h"

#include "plexus/native/detail/posix_slot_lock.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <filesystem>

#include <unistd.h>

namespace {

using plexus::shm::ring_role;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using plexus::testing::canonical_of;
using plexus::testing::sidecar_file;
using plexus::testing::k_sidecar_host;
using plexus::testing::sidecar_present;

// Where a sidecar lock file carries the slots, the newcomer creates a fresh one, wins every slot on it, proves the
// retired name still maps the ring, removes the name with its own lock file and mints; where the region's own record
// locks carry them, the retirer's hold refuses it the creator slot.
constexpr acquire_result k_newcomer_result = k_sidecar_host ? acquire_result::created : acquire_result::failed;
constexpr acquire_bound k_newcomer_bound   = k_sidecar_host ? acquire_bound::none : acquire_bound::creator_alive;

#if defined(__APPLE__)
std::filesystem::path confstr_dir(int which)
{
    std::array<char, 1024> dir{};
    const std::size_t counted = ::confstr(which, dir.data(), dir.size());
    REQUIRE((counted > 1 && counted <= dir.size()));
    return std::filesystem::path(dir.data());
}
#endif

}

// No remover deletes a guard before the name it guards, so the lock file here is deleted from outside the protocol.
TEST_CASE("sidecar_leak a newcomer that finds a retiring ring's sidecar deleted from outside leaves no sidecar behind", "[shm][sidecar_leak]")
{
    wake_bus bus;
    ring_view retirer{lifetime_fqn("sidecar_leak")};
    const std::string canonical = canonical_of(retirer.name);
    plexus::testing::leave_retire_to(retirer, bus);
    plexus::testing::retire_holding(retirer);
    REQUIRE(sidecar_present(canonical) == k_sidecar_host);
    plexus::native::detail::unlink_sidecar(canonical);
    {
        ring_holder newcomer{bus, retirer.fqn};
        REQUIRE(newcomer.acquire(ring_role::producer) == k_newcomer_result);
        CHECK(newcomer.last_bound() == k_newcomer_bound);
        newcomer.release(ring_role::producer);
    }

    retirer.control.unlink_name();
    retirer.control.release_all_slots();
    retirer.unmap();
    CHECK_FALSE(retirer.named());
    CHECK_FALSE(sidecar_present(canonical));
}

TEST_CASE("sidecar_leak a held ring keeps its sidecar lock file in the per-user cache directory and removes it at retire", "[shm][sidecar_leak]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("sidecar_dir")};
    const std::string canonical = canonical_of(view.name);
    {
        ring_holder producer{bus, view.fqn};
        REQUIRE(producer.acquire(ring_role::producer) == acquire_result::created);
        CHECK(sidecar_present(canonical) == k_sidecar_host);
#if defined(__APPLE__)
        const std::filesystem::path file = sidecar_file(canonical);
        CHECK(std::filesystem::equivalent(file.parent_path(), confstr_dir(_CS_DARWIN_USER_CACHE_DIR)));
        CHECK_FALSE(std::filesystem::exists(confstr_dir(_CS_DARWIN_USER_TEMP_DIR) / file.filename()));
#endif
        producer.release(ring_role::producer);
    }

    CHECK_FALSE(view.named());
    CHECK_FALSE(sidecar_present(canonical));
}
