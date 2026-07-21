// The relay emitter's build path, driven directly over a topic table: it lifts the origin's identity
// and every publisher topic-with-type from held state, stamps the origin universe from host-side
// config (the handshake carries none), mints a per-origin seq, and refuses to bridge an origin whose
// stamped universe the relay's own does not admit — producing no report and holding no replay entry.

#include "plexus/io/report_options.h"
#include "plexus/io/peer_report_emitter.h"

#include "plexus/io/detail/peer_report_consumers.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/topic_type_table.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/topic_hash.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/topic_declaration.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <optional>

namespace wire = plexus::wire;
using plexus::node_id;
using plexus::graph::topic_edge;
using plexus::graph::topic_role;
using plexus::graph::topic_type_table;
using plexus::io::make_report_ctx;
using plexus::io::peer_report_emitter;
using plexus::io::report_options;

namespace {

node_id make_id(std::uint8_t seed)
{
    node_id id{};
    id[0] = std::byte{seed};
    return id;
}

struct fixture
{
    topic_type_table table;
    peer_report_emitter emitter;
    node_id origin{make_id(0xC3)};

    fixture()
    {
        table.upsert(topic_edge{origin, "sensor/imu", "sensor_msgs/Imu", std::optional<std::uint64_t>{7}, topic_role::publisher});
        table.upsert(topic_edge{origin, "sensor/temp", "sensor_msgs/Temp", std::optional<std::uint64_t>{9}, topic_role::publisher});
        // A subscriber edge and a stranger's edge must NOT be re-announced as the origin's offering.
        table.upsert(topic_edge{origin, "control/cmd", "", std::nullopt, topic_role::subscriber});
        table.upsert(topic_edge{make_id(0xD4), "other/topic", "x", std::optional<std::uint64_t>{1}, topic_role::publisher});
    }

    std::optional<wire::peer_report> note(std::uint32_t universe)
    {
        std::optional<wire::peer_report> got;
        emitter.note_origin(make_report_ctx(report_options{}), origin, universe, table, [&](const wire::peer_report &pr) { got = pr; });
        return got;
    }

    bool has_topic(const wire::peer_report &pr, std::string_view fqn, std::string_view type_name) const
    {
        for(const wire::topic_declaration &td : pr.topics)
            if(td.fqn == fqn && td.type_name == type_name && td.topic_hash == wire::fqn_topic_hash(fqn) && td.state == wire::type_state::declared)
                return true;
        return false;
    }
};

}

TEST_CASE("peer_report emitter: lifts identity and publisher topics from held session state", "[graph][peer_report][emitter]")
{
    fixture f;
    const auto got = f.note(plexus::discovery::k_default_universe);

    REQUIRE(got.has_value());
    REQUIRE(got->origin == f.origin);
    REQUIRE(got->hop == 1);
    REQUIRE((got->flags & wire::k_peer_report_consent_flag) != 0);
    REQUIRE((got->flags & wire::k_peer_report_topics_flag) != 0);
    REQUIRE(got->topics.size() == 2);
    REQUIRE(f.has_topic(*got, "sensor/imu", "sensor_msgs/Imu"));
    REQUIRE(f.has_topic(*got, "sensor/temp", "sensor_msgs/Temp"));
    REQUIRE(f.emitter.reported_count() == 1);
    REQUIRE(f.emitter.reports(f.origin));
}

TEST_CASE("peer_report emitter: stamps the origin universe from host-side config", "[graph][peer_report][emitter]")
{
    fixture f;
    const auto got = f.note(plexus::discovery::k_default_universe);

    REQUIRE(got.has_value());
    REQUIRE(got->origin_universe == plexus::discovery::k_default_universe);
}

TEST_CASE("peer_report emitter: a per-origin seq is minted monotonically, never by the origin", "[graph][peer_report][emitter]")
{
    fixture f;
    const auto first  = f.note(plexus::discovery::k_default_universe);
    const auto second = f.note(plexus::discovery::k_default_universe);

    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    REQUIRE(second->seq == static_cast<std::uint16_t>(first->seq + 1));
}

TEST_CASE("peer_report emitter: refuses to bridge a foreign-universe origin", "[graph][peer_report][emitter]")
{
    fixture f;
    const auto got = f.note(plexus::discovery::k_default_universe + 1u);

    REQUIRE_FALSE(got.has_value());
    REQUIRE(f.emitter.reported_count() == 0);
    REQUIRE_FALSE(f.emitter.reports(f.origin));
}

TEST_CASE("peer_report emitter: a pattern-universe relay carries the pattern so the relay leg works", "[graph][peer_report][emitter]")
{
    fixture f;
    report_options opts;
    opts.universe_pattern = "plant/*";
    std::optional<wire::peer_report> got;
    // With a non-concrete local pattern the relay must still admit its own-universe origin (not
    // silently refuse every origin against an empty peer pattern) AND stamp the pattern on the wire.
    f.emitter.note_origin(make_report_ctx(opts), f.origin, plexus::discovery::k_default_universe, f.table,
                          [&](const wire::peer_report &pr) { got = pr; });

    REQUIRE(got.has_value());
    REQUIRE((got->flags & wire::k_peer_report_universe_pattern_flag) != 0);
    REQUIRE(got->origin_universe_pattern == "plant/*");

    // A receiver in the same pattern universe admits it through the relay; a disjoint-universe
    // receiver refuses it on the origin-universe intersect.
    REQUIRE(plexus::io::detail::report_universe_admits(make_report_ctx(opts), *got));
    report_options other;
    other.universe_pattern = "office/*";
    REQUIRE_FALSE(plexus::io::detail::report_universe_admits(make_report_ctx(other), *got));
}
