#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"

#include "plexus/io/shm/region_broker_concept.h"

#include "support/xproc_harness.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include <unistd.h>

// The crashed-creator orphan-reclaim proof (the bounded crash-orphan mitigation):
// a child CREATES a named region, stamps a sentinel byte pattern into it, and
// _exits WITHOUT releasing -- simulating a creator that crashed before its
// create-owns-unlink destructor could run, leaving an orphan live in /dev/shm.
// The parent then proves the orphan is real (a plain create returns
// already_exists) and that a create with unlink_stale_on_create RECLAIMS the
// orphan and mints a FRESH region: the new region is the parent's (ftruncate
// zero-fills it, so the orphan's stale sentinel does not leak through). Looped
// N>=100; the ctest binary is re-run >=3 times for reproducibility. This is the
// bounded mitigation that ships now; robust dead-peer arbitration is a locked
// deferral and is NOT designed here.

namespace pio = plexus::io::shm;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

// A distinguishable byte the crashed creator stamps across the orphan region;
// a non-zero pattern so a fresh (zero-filled) reclaim is unambiguous.
constexpr std::byte k_stale_byte{0xABu};
constexpr std::size_t k_region_bytes = 4096;

// Stamp the whole mapped region with the stale sentinel.
void stamp_stale(const region_handle &h)
{
    const auto bytes = h.bytes();
    std::memset(bytes.data(), static_cast<int>(k_stale_byte), bytes.size());
}

// True iff every byte of the mapped region is zero (a fresh ftruncate'd region).
bool all_zero(const region_handle &h)
{
    const auto bytes = h.bytes();
    for(const std::byte b : bytes)
        if(b != std::byte{0})
            return false;
    return true;
}

}

TEST_CASE("shm.stale_reclaim a crashed creator's orphan is reclaimed by unlink_stale_on_create",
          "[shm][stale_reclaim]")
{
    // A region name unique to this process so concurrent ctest shards never
    // collide. The bare logical name; the broker prepends its canonical prefix.
    const std::string name = "stale.reclaim." + std::to_string(::getpid());

    for(int iter = 0; iter < 100; ++iter)
    {
        // CHILD: create the region, stamp the stale sentinel, then _exit WITHOUT
        // releasing the handle -- the create-owns-unlink destructor never runs, so
        // the region is orphaned live in /dev/shm (the crashed-creator simulation).
        // PARENT: after the child has exited, reclaim the orphan and prove freshness.
        const auto outcome = plexus::testing::run_forked(
            [&]() -> bool {
                posix_shm_region_broker broker;

                // The orphan must be present: a plain create (no reclaim flag) finds
                // the child's live region and returns already_exists, never ok.
                {
                    region_handle probe;
                    if(broker.create(name, k_region_bytes, pio::create_options{}, probe) !=
                       pio::region_status::already_exists)
                        return false;
                }

                // Reclaim: create WITH unlink_stale_on_create unlinks the orphan and
                // mints a fresh region under the same name.
                region_handle fresh;
                if(broker.create(name, k_region_bytes,
                                 pio::create_options{.unlink_stale_on_create = true}, fresh) !=
                   pio::region_status::ok)
                    return false;

                // The fresh region is the PARENT's: ftruncate zero-fills it, so the
                // orphan's stale sentinel did not leak through into the new mapping.
                if(!all_zero(fresh))
                    return false;

                // The parent owns this region and unlinks it on `fresh`'s release
                // (create-owns-unlink), so no /dev/shm region leaks after the case.
                return true;
            },
            [&]() -> bool {
                posix_shm_region_broker broker;
                region_handle orphan;
                if(broker.create(name, k_region_bytes, pio::create_options{}, orphan) !=
                   pio::region_status::ok)
                    ::_exit(1);
                stamp_stale(orphan);
                // _exit HERE -- inside the child, with `orphan` still in scope -- is
                // the load-bearing crash simulation: it skips orphan's create-owns-
                // unlink destructor, so the named region stays live in /dev/shm with
                // the sentinel committed (letting the handle destruct at lambda return
                // would unlink it, defeating the orphan). The exit status (0) is the
                // child_succeeded signal the parent predicate reads.
                ::_exit(0);
            });

        REQUIRE(outcome.child_succeeded);  // the crashed-creator child stamped + orphaned
        REQUIRE(outcome.parent_succeeded); // the parent reclaimed a fresh, zero-filled region
    }
}
