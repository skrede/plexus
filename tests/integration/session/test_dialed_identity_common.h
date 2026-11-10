#ifndef HPP_GUARD_TESTS_INTEGRATION_DIALED_IDENTITY_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_DIALED_IDENTITY_COMMON_H

#include "recording_observer.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/node_id.h"

#include <chrono>

namespace dialed_identity_fixture {

using namespace routing_inproc_fixture;

constexpr auto k_ceiling = std::chrono::milliseconds(10001);

struct misdirected_card
{
    misdirected_card()
            : net()
            , rec()
            , advertised(make_id(0xC3))
    {
        net.a.add_observer(rec);
        net.a.note_peer(advertised, net.ep_b);
        net.a.reach(advertised);
        net.drive();
    }

    two_node net;
    recording_observer rec;
    plexus::node_id advertised;
};

}

#endif
