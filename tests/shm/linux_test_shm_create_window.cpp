#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/minter_entry.h"
#include "support/syscall_pause.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <vector>
#include <cstddef>
#include <cstdint>

#include <unistd.h>

#include <sys/syscall.h>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::region_status;
using plexus::shm::acquire_result;
using plexus::shm::create_options;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::syscall_arg;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using plexus::testing::syscall_pause;
using plexus::testing::native_broker;
using values = std::vector<std::uint32_t>;

// No other mapping the minter's thread makes has this length, so the filter stops only the region's own mmap.
std::size_t odd_region_bytes()
{
    return 37 * static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
}

void deliver_seven(ring_holder &producer, ring_holder &reader, ring_view &view)
{
    CHECK(reader.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(producer.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    CHECK(view.named());
    reader.release(ring_role::consumer);
}

}

// The minter's create waits in its page reservation while a second producer acquires the name.
TEST_CASE("create_window a create whose page reservation fails while a producer acquires the name leaves that producer's ring reachable", "[shm][create_window]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("create_window.reserve")};
    ring_holder producer{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    const std::size_t bytes = plexus::shm::control_region_bytes(plexus::testing::minter_geometry().cell_count);
    native_broker::region_handle minter;
    region_status minted = region_status::ok;
    acquire_result first = acquire_result::created;
    {
        syscall_pause pause{__NR_fallocate, {}, [&] { minted = native_broker{}.create(view.name, bytes, create_options{}, minter); }};
        REQUIRE(pause.wait().data.nr == __NR_fallocate);
        first = producer.acquire(ring_role::producer);
        pause.fail(ENOSPC);
    }
    CHECK(first == acquire_result::failed);
    CHECK(minted == region_status::no_space);
    CHECK(producer.acquire(ring_role::producer) == acquire_result::created);
    deliver_seven(producer, reader, view);
    producer.release(ring_role::producer);
    CHECK_FALSE(view.named());
}

// The minter's create waits in its mapping, after sizing, while a second producer adopts and recreates the name.
TEST_CASE("create_window a create whose mapping fails after another producer recreated the name leaves that producer's ring whole", "[shm][create_window]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("create_window.map")};
    ring_holder producer{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    const std::size_t bytes = odd_region_bytes();
    native_broker::region_handle minter;
    region_status minted = region_status::ok;
    {
        const std::vector<syscall_arg> args{{1, static_cast<std::uint32_t>(bytes), true}, {4, 0xffffffffu, false}};
        syscall_pause pause{__NR_mmap, args, [&] { minted = native_broker{}.create(view.name, bytes, create_options{}, minter); }};
        REQUIRE(static_cast<std::int32_t>(pause.wait().data.args[4]) != -1);
        CHECK(producer.acquire(ring_role::producer) == acquire_result::created);
        pause.fail(ENOMEM);
    }
    CHECK(minted != region_status::ok);
    deliver_seven(producer, reader, view);
    producer.release(ring_role::producer);
    CHECK_FALSE(view.named());
}
