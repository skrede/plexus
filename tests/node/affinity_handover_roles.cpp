#include "affinity_roles.h"
#include "affinity_trip_child_common.h"

#include "plexus/post.h"
#include "plexus/subscriber.h"

#include <asio/post.hpp>

#include <span>
#include <thread>
#include <cstddef>
#include <string_view>

namespace plexus::testing {

namespace {

void asio_teardown(bool release)
{
    ::asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::discovery::static_discovery disc{{}};
    affinity_asio_node node{io, disc, affinity_id(0x01), transport, plexus::node_options{}};
    plexus::subscriber<> sub{node, "affinity/topic", [](std::span<const std::byte>) {}};
    ::asio::post(io,
                 [&]
                 {
                     read_participants(node);
                     if(release)
                         plexus::release_driver(node);
                     io.stop();
                 });
    std::thread driver{[&] { io.run(); }};
    driver.join();
}

void foreign_release()
{
    asio_rig rig;
    on_driver(rig.node, [&] { read_participants(rig.node); });
    plexus::release_driver(rig.node);
}

void inproc_teardown(bool release)
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<affinity_loopback_policy> host{ex, disc, affinity_id(0x02)};
    plexus::subscriber<> sub{host.node(), "affinity/topic", [](std::span<const std::byte>) {}};
    std::thread driver{[&]
                       {
                           plexus::post(host.node(), [&] { read_participants(host.node()); });
                           ex.drain();
                           if(release)
                               plexus::release_driver(host.node());
                       }};
    driver.join();
}

void inproc_pump_handover()
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<affinity_loopback_policy> host{ex, disc, affinity_id(0x02)};
    publish_from_driver(host.node(), [&] { ex.drain(); });
}

void asio_poll_handover()
{
    ::asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::discovery::static_discovery disc{{}};
    affinity_asio_node node{io, disc, affinity_id(0x01), transport, plexus::node_options{}};
    publish_from_driver(node,
                        [&]
                        {
                            io.restart();
                            io.poll();
                        });
}

void inproc_second_stepper()
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<affinity_loopback_policy> host{ex, disc, affinity_id(0x02)};
    step_after_claimed(host.node(), [&] { ex.step(); });
}

role_outcome run_handover_misuse(std::string_view role)
{
    if(role == "asio_teardown_unreleased")
        asio_teardown(false);
    else if(role == "inproc_teardown_unreleased")
        inproc_teardown(false);
    else if(role == "foreign_release")
        foreign_release();
    else if(role == "inproc_second_stepper")
        inproc_second_stepper();
    else
        return role_outcome::unknown;
    return role_outcome::not_tripped;
}

}

role_outcome run_handover_role(std::string_view role)
{
    if(role == "asio_release_teardown")
        asio_teardown(true);
    else if(role == "inproc_release_teardown")
        inproc_teardown(true);
    else if(role == "inproc_pump_handover")
        inproc_pump_handover();
    else if(role == "asio_poll_handover")
        asio_poll_handover();
    else
        return run_handover_misuse(role);
    return role_outcome::ok;
}

}
