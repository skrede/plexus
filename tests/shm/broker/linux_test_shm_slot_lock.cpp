#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <optional>
#include <filesystem>
#include <system_error>

#include <fcntl.h>

// The Linux slot locks are open-file-description locks on a region's own /dev/shm descriptor; these cases pin them
// within one process, where two handles on one region are two descriptions and so two holders.

using plexus::shm::slot_hold;
using plexus::shm::cursor_slot;
using plexus::native::posix_shm_region_broker;

namespace {

using region_handle = posix_shm_region_broker::region_handle;

std::string unique_name(const char *tag)
{
    return std::string("slot_lock.") + tag + "." + std::to_string(plexus::testing::process_id());
}

struct region_pair
{
    explicit region_pair(const char *tag)
            : broker()
            , creator()
            , joiner()
    {
        const std::string name = unique_name(tag);
        REQUIRE(broker.create(name, 4096, plexus::shm::create_options{}, creator) == plexus::shm::region_status::ok);
        REQUIRE(broker.attach(name, joiner) == plexus::shm::region_status::ok);
    }

    ~region_pair()
    {
        creator.unlink_name();
    }

    posix_shm_region_broker broker;
    region_handle creator;
    region_handle joiner;
};

// Whether every descriptor this process holds open on the /dev/shm object is close-on-exec; nullopt when none is open.
std::optional<bool> all_close_on_exec(const std::string &object)
{
    std::optional<bool> all;
    for(const auto &entry : std::filesystem::directory_iterator("/proc/self/fd"))
    {
        std::error_code ec;
        const std::filesystem::path target = std::filesystem::read_symlink(entry.path(), ec);
        if(ec || target != object)
            continue;
        const int flags = ::fcntl(std::stoi(entry.path().filename().string()), F_GETFD);
        all             = all.value_or(true) && flags >= 0 && (flags & FD_CLOEXEC) != 0;
    }
    return all;
}

}

TEST_CASE("slot_lock a slot held through one handle reads held elsewhere through the other and not through itself", "[shm][slot_lock]")
{
    region_pair region{"elsewhere"};
    REQUIRE(region.creator.hold_slot(cursor_slot(2)) == slot_hold::held);

    CHECK(region.joiner.slot_held_elsewhere(cursor_slot(2)));
    CHECK_FALSE(region.creator.slot_held_elsewhere(cursor_slot(2)));
    CHECK_FALSE(region.joiner.slot_held_elsewhere(cursor_slot(3)));
    CHECK(region.joiner.hold_slot(cursor_slot(2)) == slot_hold::busy);
}

TEST_CASE("slot_lock closing the holder frees its slot and a released slot can be held again", "[shm][slot_lock]")
{
    region_pair region{"release"};
    REQUIRE(region.joiner.hold_slot(cursor_slot(0)) == slot_hold::held);
    REQUIRE(region.creator.slot_held_elsewhere(cursor_slot(0)));

    region.joiner = region_handle{};
    CHECK_FALSE(region.creator.slot_held_elsewhere(cursor_slot(0)));

    REQUIRE(region.creator.hold_slot(cursor_slot(0)) == slot_hold::held);
    region.creator.release_slot(cursor_slot(0));
    REQUIRE(region.broker.attach(unique_name("release"), region.joiner) == plexus::shm::region_status::ok);
    CHECK_FALSE(region.joiner.slot_held_elsewhere(cursor_slot(0)));
    CHECK(region.joiner.hold_slot(cursor_slot(0)) == slot_hold::held);
}

TEST_CASE("slot_lock every descriptor a region handle keeps open is closed on exec", "[shm][slot_lock]")
{
    region_pair region{"cloexec"};
    REQUIRE(region.joiner.hold_slot(plexus::shm::k_creator_slot) == slot_hold::held);

    const std::optional<bool> cloexec = all_close_on_exec("/dev/shm/plexus." + unique_name("cloexec"));
    REQUIRE(cloexec.has_value());
    CHECK(*cloexec);
}

TEST_CASE("slot_lock every slot is held in one step only while no other handle holds any of them", "[shm][slot_lock]")
{
    region_pair region{"all"};
    REQUIRE(region.creator.hold_slot(cursor_slot(1)) == slot_hold::held);
    REQUIRE(region.creator.hold_all_slots() == slot_hold::held);
    CHECK(region.joiner.hold_slot(plexus::shm::k_creator_slot) == slot_hold::busy);

    region.creator.release_all_slots();
    REQUIRE(region.joiner.hold_slot(cursor_slot(4)) == slot_hold::held);
    CHECK(region.creator.hold_all_slots() == slot_hold::busy);
    CHECK_FALSE(region.creator.slot_held_elsewhere(plexus::shm::k_creator_slot));
}
