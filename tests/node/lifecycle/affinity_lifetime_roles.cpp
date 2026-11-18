#include "affinity_roles.h"
#include "affinity_trip_child_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <span>
#include <cstdio>
#include <cstddef>
#include <cstdlib>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::testing {

namespace {

using lifetime_host = plexus::loopback_host<affinity_loopback_policy>;

struct lifetime_rig
{
    lifetime_rig()
            : bus()
            , ex(bus)
            , disc({})
            , host(std::in_place, ex, disc, affinity_id(0x03))
    {
    }

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex;
    plexus::discovery::static_discovery disc;
    std::optional<lifetime_host> host;
};

void destroy_in_delivery()
{
    lifetime_rig rig;
    std::optional<plexus::publisher<>> pub;
    std::optional<plexus::subscriber<>> sub;
    pub.emplace(rig.host->node(), "affinity/topic");
    sub.emplace(rig.host->node(), "affinity/topic", [&](std::span<const std::byte>) { rig.host.reset(); });
    rig.ex.drain();
    pub->publish(k_payload);
    rig.ex.drain();
}

// The sanctioned way out of a delivery: close inside it and destroy inside on_closed. A node still standing means the
// delivery never ran, so the role proves nothing and says so.
role_outcome close_in_delivery_destroy_on_closed()
{
    lifetime_rig rig;
    auto &node = rig.host->node();
    std::optional<plexus::publisher<>> pub;
    std::optional<plexus::subscriber<>> sub;
    pub.emplace(node, "affinity/topic");
    sub.emplace(node, "affinity/topic", [&](std::span<const std::byte>) { node.close(); });
    node.on_closed(
            [&]
            {
                pub.reset();
                sub.reset();
                rig.host.reset();
            });
    rig.ex.drain();
    pub->publish(k_payload);
    rig.ex.drain();
    if(!rig.host)
        return role_outcome::ok;
    std::fputs("affinity-not-destroyed\n", stderr);
    std::exit(4);
}

}

role_outcome run_lifetime_role(std::string_view role)
{
    if(role == "close_in_delivery_destroy_on_closed")
        return close_in_delivery_destroy_on_closed();
    if(role != "destroy_in_delivery")
        return role_outcome::unknown;
    destroy_in_delivery();
    return role_outcome::not_tripped;
}

}
