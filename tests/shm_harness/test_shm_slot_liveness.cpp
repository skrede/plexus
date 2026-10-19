#include "launch_helpers.h"

#include "support/xproc_process.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <optional>

#if !defined(_WIN32)
    #include <cerrno>

    #include <fcntl.h>
    #include <unistd.h>
    #include <sys/mman.h>
#endif

// The slot liveness proof seen from another process on every platform: a slot a child holds reads held while the child
// lives and free once it is killed.

namespace {

using namespace shm_harness;
using plexus::shm::slot_hold;

std::string fresh_name(const char *tag)
{
    return std::string("liveness.") + tag + "." + std::to_string(plexus::testing::process_id());
}

}

TEST_CASE("slot_liveness a slot another process holds reads held until that process is killed", "[shm][slot_liveness]")
{
    const std::string name = fresh_name("k");
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle region;
    REQUIRE(broker.create(name, 4096, plexus::shm::create_options{}, region) == plexus::shm::region_status::ok);
    const std::uint32_t slot = plexus::shm::cursor_slot(3);
    REQUIRE_FALSE(region.slot_held_elsewhere(slot));

    std::optional<plexus::testing::xproc_process> holder = plexus::testing::xproc_process::spawn("shm_harness.slot_holder", {name, std::to_string(slot)});
    REQUIRE(holder);
    REQUIRE(wait_until([&] { return region.slot_held_elsewhere(slot) || !holder->running(); }, 10s));
    REQUIRE(holder->running());
    CHECK(region.hold_slot(slot) == slot_hold::busy);

    holder->kill();
    CHECK(wait_until([&] { return !region.slot_held_elsewhere(slot); }, 5s));
    CHECK(region.hold_slot(slot) == slot_hold::held);
}

#if !defined(_WIN32)
// Which proof the POSIX backend uses on this host: a lock on the region's own descriptor, or the sidecar lock file.
TEST_CASE("slot_liveness reports whether this host takes a record lock on a shared-memory descriptor", "[shm][slot_liveness]")
{
    const std::string object = "/plexus." + fresh_name("p");
    const int fd             = ::shm_open(object.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    REQUIRE(fd >= 0);
    REQUIRE(::ftruncate(fd, 4096) == 0);
    #if defined(F_OFD_SETLK)
    struct flock lk{};
    lk.l_type          = F_WRLCK;
    lk.l_whence        = SEEK_SET;
    lk.l_len           = 1;
    const int rc       = ::fcntl(fd, F_OFD_SETLK, &lk);
    const int error_no = rc == 0 ? 0 : errno;
    WARN("F_OFD_SETLK on a shm_open descriptor: " << (rc == 0 ? "held" : "refused, errno " + std::to_string(error_no)));
    #else
    WARN("F_OFD_SETLK is not defined on this host");
    #endif
    ::close(fd);
    ::shm_unlink(object.c_str());
}
#endif
