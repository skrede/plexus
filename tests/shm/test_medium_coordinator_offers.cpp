#include "support/coordinator_lanes.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

namespace {

using plexus::io::companion_event;
using plexus::testing::lane_end;

struct sent_event
{
    std::string node_name;
    std::string fqn;
    companion_event event;

    bool operator==(const sent_event &) const = default;
};

void record_sends(lane_end &end, std::vector<sent_event> &sent)
{
    end.c.on_companion_send([&sent](std::string_view node_name, std::string_view fqn, companion_event event, std::uint32_t)
                            { sent.push_back({std::string{node_name}, std::string{fqn}, event}); });
}

}

TEST_CASE("medium_coordinator_offers a lane a re-attempt mints after the subscribe response is offered to the waiting subscriber", "[shm][medium_coordinator_offers][!shouldfail]")
{
    lane_end publisher{1};
    std::vector<sent_event> sent;
    record_sends(publisher, sent);
    publisher.offer("peer");
    REQUIRE_FALSE(publisher.c.offers("peer", "alpha"));

    publisher.c.on_retry_due();

    REQUIRE(publisher.c.offers("peer", "alpha"));
    REQUIRE(sent == std::vector<sent_event>{{"peer", "alpha", companion_event::offered}});
}
