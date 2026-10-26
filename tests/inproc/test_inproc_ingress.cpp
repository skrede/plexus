#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/process_loopback_channel.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/post.h"
#include "plexus/policy.h"
#include "plexus/node_id.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/loopback_node.h"
#include "plexus/executor_verbs.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <cstddef>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;

struct loopback_policy : plexus::executor_verbs<inproc_executor<>>
{
    using byte_channel_type = plexus::io::process_loopback_channel<loopback_policy>;
    using timer_type        = inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<loopback_policy>);

void step_until(inproc_executor<> &ex, const std::atomic<bool> &done)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while(!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        if(!ex.step())
            std::this_thread::yield();
}

}

TEST_CASE("inproc_ingress work posted from another thread runs at the next step on the stepping thread", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    std::thread::id ran_on{};

    std::thread foreign([&] { ex.post_from_thread([&] { ran_on = std::this_thread::get_id(); }); });
    foreign.join();

    REQUIRE(ran_on == std::thread::id{});
    REQUIRE(ex.step());
    REQUIRE(ran_on == std::this_thread::get_id());
    REQUIRE_FALSE(ex.step());
}

TEST_CASE("inproc_ingress inside_executor is true only inside a step on the stepping thread", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    bool inside_on_stepper = false;
    bool inside_on_foreign = true;

    ex.post(
            [&]
            {
                inside_on_stepper = ex.inside_executor();
                std::thread foreign([&] { inside_on_foreign = ex.inside_executor(); });
                foreign.join();
            });

    REQUIRE_FALSE(ex.inside_executor());
    ex.drain();
    REQUIRE(inside_on_stepper);
    REQUIRE_FALSE(inside_on_foreign);
    REQUIRE_FALSE(ex.inside_executor());
}

TEST_CASE("inproc_ingress an inproc node receives a publish posted through plexus::post from another thread", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<loopback_policy> host{ex, disc, plexus::node_id{std::byte{0x71}}};
    plexus::publisher<> pub{host.node(), "ingress"};
    std::atomic<bool> received{false};
    std::thread::id received_on{};
    plexus::subscriber<> sub{host.node(), "ingress", [&](std::span<const std::byte>)
                             {
                                 received_on = std::this_thread::get_id();
                                 received.store(true, std::memory_order_release);
                             }};
    ex.drain();

    std::thread foreign([&] { plexus::post(host.node(), [&] { pub.publish(std::array<std::byte, 1>{std::byte{0x2A}}); }); });
    step_until(ex, received);
    foreign.join();

    REQUIRE(received.load(std::memory_order_acquire));
    REQUIRE(received_on == std::this_thread::get_id());
}
