#include "plexus/post.h"
#include "plexus/node.h"
#include "plexus/policy.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"
#include "plexus/loopback_node.h"
#include "plexus/executor_verbs.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/process_loopback_channel.h"

#include "plexus/graph/participant_record.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <span>
#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace {

using plexus::inproc::inproc_executor;

using asio_node = plexus::basic_node<plexus::asio::asio_policy, plexus::asio::asio_transport>;

struct loopback_policy : plexus::executor_verbs<inproc_executor<>>
{
    using byte_channel_type = plexus::io::process_loopback_channel<loopback_policy>;
    using timer_type        = plexus::inproc::inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<loopback_policy>);

constexpr std::uint32_t k_rounds = 500;
constexpr std::array<std::byte, 2> k_payload{std::byte{0x0B}, std::byte{0xAD}};

struct tally
{
    std::atomic<bool> finished{false};
    std::atomic<std::uint32_t> done{0};
    std::atomic<std::uint32_t> on_driver{0};
    std::atomic<std::uint32_t> refused{0};
};

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

template<typename Node>
void exercise(Node &node, std::optional<plexus::publisher<>> &pub)
{
    pub->publish(k_payload);
    std::array<plexus::graph::participant_record, 4> seen{};
    node.participants(std::span<plexus::graph::participant_record>{seen});
    plexus::subscriber<> transient{node, "post/topic", [](std::span<const std::byte>) {}};
}

template<typename Node, typename Fn>
void post_counted(Node &node, tally &counts, Fn fn)
{
    if(!plexus::post(node, std::move(fn)))
        counts.refused.fetch_add(1, std::memory_order_relaxed);
}

template<typename Node, typename Inside>
void post_rounds(Node &node, std::optional<plexus::publisher<>> &pub, tally &counts, Inside inside)
{
    for(std::uint32_t i = 0; i < k_rounds; ++i)
        post_counted(node, counts,
                     [&node, &pub, &counts, inside]
                     {
                         exercise(node, pub);
                         if(inside())
                             counts.on_driver.fetch_add(1, std::memory_order_relaxed);
                         counts.done.fetch_add(1, std::memory_order_relaxed);
                     });
}

// Publishes on the stepping thread between steps, so a round that ran off the driver would race it. The publisher lives
// and dies on the stepping thread, the node's one driver.
template<typename Node>
void step_until_finished(inproc_executor<> &ex, Node &node, std::optional<plexus::publisher<>> &pub, const tally &counts)
{
    pub.emplace(node, "post/topic");
    while(!counts.finished.load(std::memory_order_acquire))
    {
        pub->publish(k_payload);
        ex.drain();
    }
    pub.reset();
}

// Publishes on the executor thread between posted rounds, so a round that ran off the driver would race it.
struct asio_churn
{
    ::asio::io_context &io;
    std::optional<plexus::publisher<>> &pub;

    void operator()() const
    {
        if(!pub)
            return;
        pub->publish(k_payload);
        ::asio::post(io, *this);
    }
};

// Queued before any thread starts, so the publisher exists before the first foreign round runs.
void seed_churn(::asio::io_context &io, asio_node &node, std::optional<plexus::publisher<>> &pub)
{
    ::asio::post(io,
                 [&io, &node, &pub]
                 {
                     pub.emplace(node, "post/topic");
                     asio_churn{io, pub}();
                 });
}

void post_rounds_then_stop(::asio::io_context &io, asio_node &node, std::optional<plexus::publisher<>> &pub, tally &counts)
{
    post_rounds(node, pub, counts, [&io] { return plexus::asio::asio_policy::inside_executor(io); });
    post_counted(node, counts,
                 [&io, &pub]
                 {
                     pub.reset();
                     io.stop();
                 });
}

}

TEST_CASE("post_from_thread a foreign thread's posted publishes, reads and handle lifetimes run on the asio executor thread race-free", "[node]")
{
    ::asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::discovery::static_discovery disc{{}};
    asio_node node{io, disc, make_id(0x01), transport, plexus::node_options{}};
    std::optional<plexus::publisher<>> pub;
    tally counts;
    auto guard = ::asio::make_work_guard(io);

    seed_churn(io, node, pub);
    std::thread executor([&] { io.run(); });
    std::thread foreign([&] { post_rounds_then_stop(io, node, pub, counts); });
    foreign.join();
    executor.join();

    REQUIRE(counts.refused.load() == 0);
    REQUIRE(counts.done.load() == k_rounds);
    REQUIRE(counts.on_driver.load() == k_rounds);
}

TEST_CASE("post_from_thread a foreign thread's posted publishes, reads and handle lifetimes run on the inproc stepping thread race-free", "[node]")
{
    plexus::inproc::inproc_bus<> bus;
    inproc_executor<> ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<loopback_policy> host{ex, disc, make_id(0x02)};
    std::optional<plexus::publisher<>> pub;
    tally counts;

    std::thread stepper([&] { step_until_finished(ex, host.node(), pub, counts); });
    std::thread foreign(
            [&]
            {
                post_rounds(host.node(), pub, counts, [&ex] { return ex.inside_executor(); });
                post_counted(host.node(), counts, [&counts] { counts.finished.store(true, std::memory_order_release); });
            });
    foreign.join();
    stepper.join();

    REQUIRE(counts.refused.load() == 0);
    REQUIRE(counts.done.load() == k_rounds);
    REQUIRE(counts.on_driver.load() == k_rounds);
}
