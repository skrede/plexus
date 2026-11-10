#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_ROLES_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_ROLES_H

#include "plexus/post.h"
#include "plexus/publisher.h"

#include "plexus/graph/participant_record.h"

#include <span>
#include <array>
#include <atomic>
#include <cstdio>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::testing {

enum class role_outcome : std::uint8_t
{
    unknown,
    ok,
    not_tripped
};

inline constexpr std::array<std::byte, 2> k_payload{std::byte{0x0A}, std::byte{0xFF}};

// A refused post would leave the role waiting on work that never runs, so the child says so and exits 3.
template<typename Node, typename Fn>
void post_or_exit(Node &node, Fn fn)
{
    if(plexus::post(node, std::move(fn)))
        return;
    std::fputs("affinity-post-refused\n", stderr);
    std::exit(3);
}

template<typename Node>
void read_participants(Node &node)
{
    std::array<plexus::graph::participant_record, 4> seen{};
    node.participants(std::span<plexus::graph::participant_record>{seen});
}

template<typename Node, typename Drive>
void publish_from_driver(Node &node, Drive drive)
{
    std::optional<plexus::publisher<>> pub;
    pub.emplace(node, "affinity/topic");
    std::thread driver{[&]
                       {
                           for(std::int32_t round = 0; round < 10; ++round)
                           {
                               drive();
                               pub->publish(k_payload);
                           }
                           pub.reset();
                       }};
    driver.join();
}

// The first stepper is joined before the caller steps, so a missing claim check shows as a clean return, not a race.
template<typename Node, typename Step>
void step_after_claimed(Node &node, Step step)
{
    std::atomic<bool> ran{false};
    post_or_exit(node,
                 [&]
                 {
                     read_participants(node);
                     ran.store(true, std::memory_order_release);
                 });
    std::thread first{[&]
                      {
                          while(!ran.load(std::memory_order_acquire))
                              step();
                      }};
    first.join();
    step();
}

role_outcome run_handover_role(std::string_view role);

role_outcome run_freertos_role(std::string_view role);

}

#endif
