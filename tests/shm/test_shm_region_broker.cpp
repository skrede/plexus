#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>

#include <sys/mman.h>

// The portable POSIX region-broker negative-path behaviors: an oversize create fast-fails
// (too_large) with no region minted, an attach of a name that was never created returns
// not_found, and a sliced/empty name is rejected. These assertions read only the broker's
// return status and handle validity -- no /dev/shm path inspection -- so they hold on every
// POSIX host (Linux and macOS alike; macOS shm_open objects are not path-visible). The
// /dev/shm-view assertions (0600 mode, create-owns-unlink) live in the Linux-only companion.

using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;
namespace pio = plexus::shm;

static_assert(pio::region_broker<posix_shm_region_broker>, "the broker test fixes the concept satisfaction as a compile gate");

namespace {

// A bare region name unique to this process (the broker prefixes /plexus.).
std::string unique_name(const char *tag)
{
    return std::string("regbroker.") + tag + "." + std::to_string(plexus::testing::process_id());
}

}

TEST_CASE("shm.oversize a create above the ceiling fast-fails with no region created", "[shm][oversize]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("oversize");
    ::shm_unlink((std::string("/plexus.") + name).c_str());

    region_handle h;
    const std::size_t over = posix_shm_region_broker::k_max_region_size + 1;
    // The ceiling is checked BEFORE the name is sanitized and BEFORE any shm_open, so an
    // over-ceiling create returns too_large and mints no region regardless of the host's shm
    // namespace -- the portable behavioral guarantee. The /dev/shm-view proof that no object
    // was left behind lives in the Linux-only companion.
    REQUIRE(broker.create(name, over, pio::create_options{}, h) == pio::region_status::too_large);
    REQUIRE_FALSE(h.valid());
}

TEST_CASE("shm.region_broker attach of a missing name returns not_found", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("missing");
    ::shm_unlink((std::string("/plexus.") + name).c_str());

    region_handle h;
    REQUIRE(broker.attach(name, h) == pio::region_status::not_found);
    REQUIRE_FALSE(h.valid());
}

TEST_CASE("shm.region_broker rejects a sliced or empty name", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    region_handle h;
    REQUIRE(broker.create("", 4096, pio::create_options{}, h) == pio::region_status::failed);
    REQUIRE(broker.create("has/slash", 4096, pio::create_options{}, h) == pio::region_status::failed);
}
