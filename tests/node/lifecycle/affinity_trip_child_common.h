#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_TRIP_CHILD_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_AFFINITY_TRIP_CHILD_COMMON_H

#include "affinity_roles.h"

#include "plexus/node.h"
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

#include "plexus/discovery/static_discovery.h"

#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::testing {

using affinity_asio_node = plexus::basic_node<plexus::asio::asio_policy, plexus::asio::asio_transport>;

struct affinity_loopback_policy : plexus::executor_verbs<plexus::inproc::inproc_executor<>>
{
    using byte_channel_type = plexus::io::process_loopback_channel<affinity_loopback_policy>;
    using timer_type        = plexus::inproc::inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;
};

inline plexus::node_id affinity_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Hands fn to the node's driver through plexus::post and waits until it has run there.
template<typename Node, typename Fn>
void on_driver(Node &node, Fn fn)
{
    std::atomic<bool> done{false};
    post_or_exit(node,
                 [&]
                 {
                     fn();
                     done.store(true, std::memory_order_release);
                 });
    while(!done.load(std::memory_order_acquire))
        std::this_thread::yield();
}

struct asio_rig
{
    asio_rig()
            : io()
            , transport(io)
            , disc({})
            , node(io, disc, affinity_id(0x01), transport, plexus::node_options{})
            , guard(::asio::make_work_guard(io))
            , worker([this] { io.run(); })
    {
    }

    ~asio_rig()
    {
        guard.reset();
        io.stop();
        worker.join();
    }

    ::asio::io_context io;
    plexus::asio::asio_transport transport;
    plexus::discovery::static_discovery disc;
    affinity_asio_node node;
    ::asio::executor_work_guard<::asio::io_context::executor_type> guard;
    std::thread worker;
};

struct inproc_rig
{
    inproc_rig()
            : bus()
            , ex(bus)
            , disc({})
            , host(ex, disc, affinity_id(0x02))
            , stop(false)
            , stepper(
                      [this]
                      {
                          while(!stop.load(std::memory_order_acquire))
                              ex.step();
                      })
    {
    }

    ~inproc_rig()
    {
        stop.store(true, std::memory_order_release);
        stepper.join();
    }

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex;
    plexus::discovery::static_discovery disc;
    plexus::loopback_host<affinity_loopback_policy> host;
    std::atomic<bool> stop;
    std::thread stepper;
};

role_outcome run_endpoint_role(std::string_view role);
}

#endif
