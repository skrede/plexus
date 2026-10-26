#include "affinity_roles.h"
#include "affinity_trip_child_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <asio/post.hpp>

#include <span>
#include <array>
#include <atomic>
#include <cstdio>
#include <thread>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <string_view>

#if !defined(_WIN32)
    #include <sys/resource.h>
#endif

using plexus::testing::asio_rig;
using plexus::testing::on_driver;
using plexus::testing::inproc_rig;
using plexus::testing::role_outcome;
using plexus::testing::read_participants;

namespace {

constexpr std::array<std::byte, 2> k_payload{std::byte{0x0A}, std::byte{0xFF}};

void foreign_publish()
{
    asio_rig rig;
    plexus::publisher<> pub{rig.node, "affinity/topic"};
    on_driver(rig.node, [&] { read_participants(rig.node); });
    pub.publish(k_payload);
}

void foreign_member()
{
    asio_rig rig;
    on_driver(rig.node, [&] { read_participants(rig.node); });
    read_participants(rig.node);
}

// The first runner is held inside a handler, so the posted read can only run on the second.
void second_runner()
{
    asio_rig rig;
    on_driver(rig.node, [&] { read_participants(rig.node); });
    std::atomic<bool> held{false};
    std::atomic<bool> release{false};
    ::asio::post(rig.io,
                 [&]
                 {
                     held.store(true, std::memory_order_release);
                     while(!release.load(std::memory_order_acquire))
                         std::this_thread::yield();
                 });
    while(!held.load(std::memory_order_acquire))
        std::this_thread::yield();
    std::thread second([&] { rig.io.run(); });
    on_driver(rig.node, [&] { read_participants(rig.node); });
    release.store(true, std::memory_order_release);
    rig.io.stop();
    second.join();
}

void foreign_handle_destroy()
{
    asio_rig rig;
    std::optional<plexus::subscriber<>> sub;
    on_driver(rig.node, [&] { sub.emplace(rig.node, "affinity/topic", [](std::span<const std::byte>) {}); });
    sub.reset();
}

void inproc_foreign()
{
    inproc_rig rig;
    on_driver(rig.host.node(), [&] { read_participants(rig.host.node()); });
    read_participants(rig.host.node());
}

template<typename Node>
void exercise_through_post(Node &node)
{
    std::optional<plexus::publisher<>> pub;
    on_driver(node,
              [&]
              {
                  pub.emplace(node, "affinity/topic");
                  read_participants(node);
              });
    on_driver(node,
              [&]
              {
                  pub->publish(k_payload);
                  plexus::subscriber<> transient{node, "affinity/topic", [](std::span<const std::byte>) {}};
              });
    on_driver(node, [&] { pub.reset(); });
}

void via_post()
{
    asio_rig asio;
    exercise_through_post(asio.node);
    inproc_rig inproc;
    exercise_through_post(inproc.host.node());
}

role_outcome run_misuse(std::string_view role)
{
    if(role == "foreign_publish")
        foreign_publish();
    else if(role == "foreign_member")
        foreign_member();
    else if(role == "second_runner")
        second_runner();
    else if(role == "foreign_handle_destroy")
        foreign_handle_destroy();
    else if(role == "inproc_foreign")
        inproc_foreign();
    else
        return role_outcome::unknown;
    return role_outcome::not_tripped;
}

role_outcome run_role(std::string_view role)
{
    if(role == "via_post")
    {
        via_post();
        return role_outcome::ok;
    }
    role_outcome outcome = run_misuse(role);
    if(outcome == role_outcome::unknown)
        outcome = plexus::testing::run_handover_role(role);
    if(outcome == role_outcome::unknown)
        outcome = plexus::testing::run_freertos_role(role);
    return outcome;
}

}

// A misuse that returns prints affinity-not-tripped and exits 0, which the parent test reads as the check missing. An
// expected abort leaves no core file behind.
int main(int argc, char **argv)
{
#if defined(_MSC_VER)
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#elif !defined(_WIN32)
    const rlimit no_core{0, 0};
    setrlimit(RLIMIT_CORE, &no_core);
#endif
    const role_outcome outcome = run_role(argc > 1 ? argv[1] : "");
    if(outcome == role_outcome::unknown)
        return 2;
    std::puts(outcome == role_outcome::ok ? "affinity-ok" : "affinity-not-tripped");
    return 0;
}
