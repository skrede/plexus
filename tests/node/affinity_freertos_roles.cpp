#include "affinity_roles.h"

#include "support/freertos_loopback_policy.h"

#include "plexus/post.h"
#include "plexus/node_id.h"
#include "plexus/subscriber.h"
#include "plexus/loopback_node.h"

#include "plexus/graph/participant_record.h"

#include "plexus/freertos/freertos_executor.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <array>
#include <thread>
#include <cstddef>
#include <string_view>

namespace plexus::testing {

namespace {

using freertos_host = plexus::loopback_host<freertos_loopback_policy>;

void read_participants(plexus::loopback_node<freertos_loopback_policy> &node)
{
    std::array<plexus::graph::participant_record, 4> seen{};
    node.participants(std::span<plexus::graph::participant_record>{seen});
}

// The host shim reports one task handle for every thread, so this role proves the release reaches the FreeRTOS
// executor's record through its policy, not that the record tells the two threads apart.
void freertos_release_teardown()
{
    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc{{}};
    plexus::node_id id{};
    id[0] = std::byte{0x03};
    freertos_host host{ex, disc, id};
    plexus::subscriber<> sub{host.node(), "affinity/topic", [](std::span<const std::byte>) {}};
    std::thread driver{[&]
                       {
                           plexus::post(host.node(), [&] { read_participants(host.node()); });
                           ex.drain();
                           plexus::release_driver(host.node());
                       }};
    driver.join();
}

}

role_outcome run_freertos_role(std::string_view role)
{
    if(role != "freertos_release_teardown")
        return role_outcome::unknown;
    freertos_release_teardown();
    return role_outcome::ok;
}

}
