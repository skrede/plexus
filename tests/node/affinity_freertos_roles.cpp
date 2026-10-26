#include "affinity_roles.h"

#include "support/freertos_loopback_policy.h"

#include "plexus/post.h"
#include "plexus/node_id.h"
#include "plexus/subscriber.h"
#include "plexus/loopback_node.h"

#include "plexus/freertos/freertos_executor.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <thread>
#include <cstddef>
#include <string_view>

namespace plexus::testing {

namespace {

using freertos_host = plexus::loopback_host<freertos_loopback_policy>;

plexus::node_id freertos_id()
{
    plexus::node_id id{};
    id[0] = std::byte{0x03};
    return id;
}

void freertos_release_teardown()
{
    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc{{}};
    freertos_host host{ex, disc, freertos_id()};
    plexus::subscriber<> sub{host.node(), "affinity/topic", [](std::span<const std::byte>) {}};
    std::thread driver{[&]
                       {
                           plexus::post(host.node(), [&] { read_participants(host.node()); });
                           ex.drain();
                           plexus::release_driver(host.node());
                       }};
    driver.join();
}

void freertos_pump_handover()
{
    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc{{}};
    freertos_host host{ex, disc, freertos_id()};
    publish_from_driver(host.node(), [&] { ex.drain(); });
}

void freertos_second_pumper()
{
    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc{{}};
    freertos_host host{ex, disc, freertos_id()};
    step_after_claimed(host.node(), [&] { ex.pump(); });
}

}

role_outcome run_freertos_role(std::string_view role)
{
    if(role == "freertos_second_pumper")
    {
        freertos_second_pumper();
        return role_outcome::not_tripped;
    }
    if(role == "freertos_release_teardown")
        freertos_release_teardown();
    else if(role == "freertos_pump_handover")
        freertos_pump_handover();
    else
        return role_outcome::unknown;
    return role_outcome::ok;
}

}
