#include "support/xproc_control.h"
#include "support/xproc_process.h"
#include "support/xproc_child_main.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <optional>

using plexus::testing::xproc_shared;
using plexus::testing::xproc_process;
using plexus::testing::scoped_shm_name;

namespace {

using namespace std::chrono_literals;

struct control_block
{
    std::atomic<std::uint64_t> counter;
    std::atomic<std::uint64_t> value;
};

constexpr std::uint64_t k_stored_value = 0x5EED'C0DE'0000'0001u;

bool exit_ok_child(const std::vector<std::string> &)
{
    return true;
}

// The deadline keeps a child whose parent died before killing it from spinning forever.
bool spin_child(const std::vector<std::string> &args)
{
    auto shared = xproc_shared<control_block>::attach(args.at(0));
    if(!shared)
        return false;
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while(std::chrono::steady_clock::now() < deadline)
    {
        shared->get().counter.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(100us);
    }
    return false;
}

bool store_child(const std::vector<std::string> &args)
{
    auto shared = xproc_shared<control_block>::attach(args.at(0));
    if(!shared)
        return false;
    shared->get().value.store(k_stored_value, std::memory_order_release);
    return true;
}

[[maybe_unused]] const bool s_exit_ok_registered = plexus::testing::register_xproc_child("shm.xproc.exit_ok", &exit_ok_child);
[[maybe_unused]] const bool s_spin_registered    = plexus::testing::register_xproc_child("shm.xproc.spin", &spin_child);
[[maybe_unused]] const bool s_store_registered   = plexus::testing::register_xproc_child("shm.xproc.store", &store_child);

bool counter_advances(const control_block &block, std::chrono::milliseconds bound)
{
    const std::uint64_t start = block.counter.load(std::memory_order_relaxed);
    const auto deadline       = std::chrono::steady_clock::now() + bound;
    while(block.counter.load(std::memory_order_relaxed) == start)
    {
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

}

TEST_CASE("xproc_process a child's exit status is read back within the bound", "[shm][xproc]")
{
    std::optional<xproc_process> child = xproc_process::spawn("shm.xproc.exit_ok", {});
    REQUIRE(child);

    REQUIRE(child->wait_for(5s) == std::optional<std::int32_t>(0));
    REQUIRE_FALSE(child->running());
}

TEST_CASE("xproc_process a running child is killed and reaped", "[shm][xproc]")
{
    const scoped_shm_name name("xproc.kill.");
    auto shared = xproc_shared<control_block>::create(name.str());
    REQUIRE(shared);
    std::optional<xproc_process> child = xproc_process::spawn("shm.xproc.spin", {name.str()});
    REQUIRE(child);
    REQUIRE(counter_advances(shared->get(), 5s));
    REQUIRE(child->running());

    child->kill();
    const std::optional<std::int32_t> status = child->wait_for(5s);
    REQUIRE(status);
    REQUIRE(*status != 0);
    REQUIRE_FALSE(child->running());
}

TEST_CASE("xproc_process a handle destroyed while its child runs kills the child", "[shm][xproc]")
{
    const scoped_shm_name name("xproc.destroy.");
    auto shared = xproc_shared<control_block>::create(name.str());
    REQUIRE(shared);
    {
        std::optional<xproc_process> child = xproc_process::spawn("shm.xproc.spin", {name.str()});
        REQUIRE(child);
        REQUIRE(counter_advances(shared->get(), 5s));
    }

    REQUIRE_FALSE(counter_advances(shared->get(), 500ms));
}

TEST_CASE("xproc_control a counter written by a child is read by the parent", "[shm][xproc]")
{
    const scoped_shm_name name("xproc.control.");
    auto shared = xproc_shared<control_block>::create(name.str());
    REQUIRE(shared);
    std::optional<xproc_process> child = xproc_process::spawn("shm.xproc.store", {name.str()});
    REQUIRE(child);

    REQUIRE(child->wait_for(5s) == std::optional<std::int32_t>(0));
    REQUIRE(shared->get().value.load(std::memory_order_acquire) == k_stored_value);
}

#if !defined(_WIN32)
// A live handle stands in for a crashed run's leftover: a POSIX name stays linked until something unlinks it, while a
// Windows object dies with its last handle.
TEST_CASE("xproc_control a scoped name removes the broker's object left by an earlier run", "[shm][xproc]")
{
    const std::string prefix = "xproc.leftover.";
    const std::string bare   = prefix + std::to_string(plexus::testing::process_id());
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle leftover;
    plexus::native::shm_region_handle inside;
    REQUIRE(broker.create(bare, 64, {}, leftover) == plexus::shm::region_status::ok);
    {
        const scoped_shm_name name(prefix);
        plexus::native::shm_region_handle probe;
        REQUIRE(broker.attach(name.str(), probe) != plexus::shm::region_status::ok);
        REQUIRE(broker.create(name.str(), 64, {}, inside) == plexus::shm::region_status::ok);
    }

    plexus::native::shm_region_handle probe;
    REQUIRE(broker.attach(bare, probe) != plexus::shm::region_status::ok);
}
#endif

int main(int argc, char **argv)
{
    plexus::testing::xproc_capture_argv(argc, argv);
    plexus::testing::xproc_child_main(argc, argv);
    return Catch::Session().run(argc, argv);
}
