#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/native/detail/posix_shm_status.h"

#include "plexus/shm/region_broker_concept.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <string>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

// The Linux-only region-broker behaviors that verify the broker THROUGH the /dev/shm tmpfs
// view of POSIX shm: the 0600 owner-only mode of a minted region, the name contract (no
// release removes the name; unlink_name does, under a live mapping), and unlink_stale_on_create
// reclaiming a pre-existing orphan. These stat()/access() the object at /dev/shm/plexus.<name>,
// which exists only on Linux -- macOS shm_open objects are not path-visible, so the same
// broker guarantees are proven there by the cross-process shm proofs instead. The portable
// negative-path cases live in test_shm_region_broker.cpp.

using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;
namespace pio = plexus::shm;

namespace {

std::string unique_name(const char *tag)
{
    return std::string("regbroker.") + tag + "." + std::to_string(plexus::testing::process_id());
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

struct descriptor_scan
{
    std::int32_t count;
    bool all_cloexec;
};

descriptor_scan scan_descriptors(const std::string &bare)
{
    descriptor_scan scan{0, true};
    std::error_code ec;
    for(const auto &entry : std::filesystem::directory_iterator("/proc/self/fd", ec))
    {
        if(std::filesystem::read_symlink(entry.path(), ec) != dev_shm_path(bare))
            continue;
        const int fd = std::stoi(entry.path().filename().string());
        ++scan.count;
        scan.all_cloexec = scan.all_cloexec && (::fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0;
    }
    return scan;
}

}

TEST_CASE("shm.region_broker creates a 0600 region, attaches it, and keeps the name until unlink_name removes it", "[shm][region_broker]")
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
    }

    REQUIRE(exists(name));
    region_handle survivor;
    REQUIRE(broker.attach(name, survivor) == pio::region_status::ok);
    survivor.unlink_name();
    REQUIRE_FALSE(exists(name));
    REQUIRE(survivor.bytes()[0] == std::byte{0xAB});
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

    REQUIRE(exists(name));
    REQUIRE(broker.unlink(name) == pio::region_status::ok);
}

TEST_CASE("shm.region_broker opens created and attached regions close-on-exec", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    const std::string name = unique_name("cloexec");
    ::shm_unlink((std::string("/plexus.") + name).c_str());

    region_handle creator;
    REQUIRE(broker.create(name, 4096, pio::create_options{}, creator) == pio::region_status::ok);
    region_handle attacher;
    REQUIRE(broker.attach(name, attacher) == pio::region_status::ok);

    const descriptor_scan scan = scan_descriptors(name);
    REQUIRE(broker.unlink(name) == pio::region_status::ok);
    REQUIRE(scan.count == 2);
    REQUIRE(scan.all_cloexec);
}

TEST_CASE("shm.region_broker reports exhausted shared memory as no space", "[shm][region_broker]")
{
    REQUIRE(plexus::native::detail::to_status(plexus::native::detail::map_errno(ENOSPC)) == pio::region_status::no_space);
}

TEST_CASE("shm.region_broker unlinks a region by its bare name", "[shm][region_broker]")
{
    posix_shm_region_broker broker;
    const std::string name      = unique_name("unlink");
    const std::string canonical = std::string("/plexus.") + name;
    const int planted           = ::shm_open(canonical.c_str(), O_CREAT | O_RDWR, 0600);
    REQUIRE(planted >= 0);
    ::close(planted);
    REQUIRE(exists(name));

    REQUIRE(broker.unlink(name) == pio::region_status::ok);
    REQUIRE_FALSE(exists(name));
    REQUIRE(broker.unlink(name) == pio::region_status::not_found);
}
