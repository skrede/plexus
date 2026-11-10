#include "test_proven_identity_common.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace proven_identity_fixture;

TEST_CASE("proven identity: three inbound reconnects report every edge, verdict and delivered source under the dialer's own id", "[integration][proven_identity]")
{
    manual_clock::reset();
    proven_net net;
    const std::size_t slots = net.b.registry().slot_count();
    net.connect();
    for(std::int32_t cycle = 0; cycle < 4; ++cycle)
    {
        CHECK(net.b.is_connected(net.id_a));
        CHECK(net.b.registry().slot_count() == slots + 1);
        net.b.subscribe(net.id_a, k_topic);
        net.ex.drain();
        net.publish();
        if(cycle < 3)
            net.drop_and_redial();
    }
    REQUIRE(net.sources.size() == 4);
    REQUIRE(!net.seen.ids.empty());
    CHECK(std::ranges::all_of(net.seen.ids, [&net](const node_id &id) { return id == net.id_a; }));
    CHECK(std::ranges::all_of(net.sources, [&net](const node_id &id) { return id == net.id_a; }));
    CHECK(std::ranges::none_of(net.seen.ids, provisional));
}

TEST_CASE("proven identity: a subscribe made by the dialer's id is replayed when the dialer reconnects", "[integration][proven_identity]")
{
    manual_clock::reset();
    proven_net net;
    net.connect();
    net.b.subscribe(net.id_a, k_topic);
    net.ex.drain();
    net.publish();
    REQUIRE(net.sources.size() == 1);

    net.drop_and_redial();
    net.publish();
    CHECK(net.sources.size() == 2);
}

TEST_CASE("proven identity: a refused inbound handshake reports no identity", "[integration][proven_identity]")
{
    manual_clock::reset();
    proven_net net;
    transport_t transport_c{net.ex, net.bus};
    engine c(transport_c, net.ex, older_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, net.sink, false);

    c.dial(net.ep_b);
    net.ex.drain();
    REQUIRE(net.seen.rejected.size() == 1);
    CHECK(net.seen.rejected.front() == node_id{});
    CHECK(net.seen.rejected_names.front().empty());
    CHECK(std::ranges::none_of(net.seen.verdicts, provisional));
    c.hang_up(net.ep_b);
    net.ex.drain();
}

// a's redial waits for its backoff, so b's own dial is the only one in flight.
TEST_CASE("proven identity: a reach for a dialer whose accepted slot is still draining dials it at once", "[integration][proven_identity]")
{
    manual_clock::reset();
    proven_net net;
    net.connect();
    engine::session_type *accepted = net.accepted_from_a();
    REQUIRE(accepted != nullptr);

    accepted->tear_down();
    net.b.note_peer(net.id_a, net.ep_a);
    net.b.reach(net.id_a);
    net.ex.drain();
    CHECK(net.b.is_connected(net.id_a));
    CHECK(net.b.registry().slot_count() == 1);
}
