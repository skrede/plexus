// The single source compiled by both the host build and the ESP-IDF xtensa cross-compile: the
// byte-identical proof that the graph schema is pure logic with no platform #ifdef. participant_record
// holds io::endpoint (std::string), which is NOT constexpr-constructible, so the static_asserts
// exercise only the literal parts (the observation enum, the reserved optionals); the runtime leg
// builds a full participant_record from an endpoint.

#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <optional>

namespace
{

using plexus::graph::observation;
using plexus::graph::participant_record;
using plexus::graph::provenance;
using plexus::graph::route;

constexpr bool observation_values_distinct()
{
    return observation::directly_observed != observation::reported;
}

constexpr bool direct_only_provenance_has_no_reporter()
{
    const provenance origin{observation::directly_observed, std::nullopt};
    return origin.how == observation::directly_observed && origin.reporter == std::nullopt;
}

static_assert(observation_values_distinct());
static_assert(direct_only_provenance_has_no_reporter());

}

int main()
{
    const plexus::node_id id{};
    const participant_record rec{
        id,
        route{plexus::io::endpoint{"udp", "203.0.113.7:9000"}, std::nullopt},
        provenance{observation::directly_observed, std::nullopt}};

    const bool direct_only = rec.reach.via == std::nullopt
                             && rec.origin.how == observation::directly_observed
                             && rec.origin.reporter == std::nullopt;

    return (observation_values_distinct() && direct_only_provenance_has_no_reporter() && direct_only)
                   ? 0
                   : 1;
}
