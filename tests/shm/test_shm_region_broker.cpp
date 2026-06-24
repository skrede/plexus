#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <string>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

// The POSIX region broker: create mints a 0600 /dev/shm region the creator's last
// release unlinks; attach maps the same name; unlink_stale_on_create reclaims a
// pre-existing orphan; an oversize create fast-fails (too_large) with NO region
// created. The broker satisfies the core region_broker concept (the static_assert
// in the header). These cases run in-process (the cross-process round-trip is a
// separate xproc proof).

using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;
namespace pio = plexus::shm;

static_assert(pio::region_broker<posix_shm_region_broker>, "the broker test fixes the concept satisfaction as a compile gate");

namespace {

// A bare region name unique to this process (the broker prefixes /plexus.). The
// canonical /dev/shm path is /dev/shm/plexus.<name>.
std::string unique_name(const char *tag)
{
    return std::string("regbroker.") + tag + "." + std::to_string(::getpid());
}

std::string dev_shm_path(const std::string &bare)
{
    return std::string("/dev/shm/plexus.") + bare;
}

// The mode bits (low 12) of a /dev/shm object, or -1 if it does not exist.
int mode_of(const std::string &bare)
{
    struct stat st{};
    if(::stat(dev_shm_path(bare).c_str(), &st) != 0)
        return -1;
    return static_cast<int>(st.st_mode & 07777);
}

bool exists(const std::string &bare)
{
    return ::access(dev_shm_path(bare).c_str(), F_OK) == 0;
}

}

TEST_CASE("shm.region_broker creates a 0600 region, attaches it, and unlinks on release", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("create");

    // Hygiene: a prior crashed run may have orphaned the name.
    ::shm_unlink(dev_shm_path(name).substr(strlen("/dev/shm")).c_str());

    {
        region_handle creator;
        REQUIRE(broker.create(name, 4096, pio::create_options{}, creator) == pio::region_status::ok);
        REQUIRE(creator.valid());
        REQUIRE(creator.size() >= 4096);

        // The region exists in /dev/shm with owner-only 0600 perms (the same-uid
        // access floor) -- subject to the process umask, which can only CLEAR
        // bits, never add group/other access. So group/other must be clear.
        const int mode = mode_of(name);
        REQUIRE(mode >= 0);
        REQUIRE((mode & 0077) == 0); // no group/other bits

        // A second attach from the same process maps the same region.
        region_handle attacher;
        REQUIRE(broker.attach(name, attacher) == pio::region_status::ok);
        REQUIRE(attacher.valid());

        // Write through the attacher, read through the creator: same backing page.
        attacher.bytes()[0] = std::byte{0xAB};
        REQUIRE(creator.bytes()[0] == std::byte{0xAB});
        // The attacher munmaps but NEVER unlinks: the name survives its release.
    }

    // The creator's release unlinked the name (create-owns-unlink).
    REQUIRE_FALSE(exists(name));
}

TEST_CASE("shm.region_broker unlink_stale_on_create reclaims a pre-existing orphan", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("stale");

    // Plant a stale orphan: an exclusive create that we deliberately leak (the fd
    // closes but the name persists -- a crashed creator's footprint).
    const std::string canonical = std::string("/plexus.") + name;
    const int orphan            = ::shm_open(canonical.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    REQUIRE(orphan >= 0);
    ::close(orphan);
    REQUIRE(exists(name));

    // A plain create collides with the orphan.
    {
        region_handle h;
        REQUIRE(broker.create(name, 4096, pio::create_options{}, h) == pio::region_status::already_exists);
    }

    // With unlink_stale_on_create the broker reclaims the orphan and creates fresh.
    {
        region_handle h;
        pio::create_options opts;
        opts.unlink_stale_on_create = true;
        REQUIRE(broker.create(name, 4096, opts, h) == pio::region_status::ok);
        REQUIRE(h.valid());
    }

    REQUIRE_FALSE(exists(name));     // the fresh creator unlinked on release
    ::shm_unlink(canonical.c_str()); // belt-and-braces
}

TEST_CASE("shm.oversize a create above the ceiling fast-fails with no region created", "[shm][oversize]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("oversize");
    ::shm_unlink((std::string("/plexus.") + name).c_str());

    region_handle h;
    const std::size_t over = posix_shm_region_broker::k_max_region_size + 1;
    REQUIRE(broker.create(name, over, pio::create_options{}, h) == pio::region_status::too_large);
    REQUIRE_FALSE(h.valid());

    // The fast-fail happened BEFORE any shm_open: no region exists under the name.
    REQUIRE_FALSE(exists(name));
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
