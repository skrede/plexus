#include "support/graph_change_inproc.h"

#include "plexus/target_profile.h"

#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/peer_liveliness_event.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace plexus::testing::graph_change_fixture;

namespace {

// The engine keeps its peer table private, so the storage mirrors its own tally after every put.
struct tallied_storage : plexus::io::fixed_peer_storage<8>
{
    static inline std::size_t tally = 0;

    bool put(const plexus::node_id &id, const plexus::io::endpoint &ep, std::uint64_t now)
    {
        const bool admitted = fixed_peer_storage::put(id, ep, now);
        tally               = dropped();
        return admitted;
    }
};

using tallied_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, tallied_storage, plexus::graph::fixed_topic_storage<8>>;
using fixed_arbiter_engine =
        plexus::io::routing_engine<manual_policy, transport_t, manual_clock, tallied_storage, plexus::graph::fixed_topic_storage<8>, plexus::fixed_liveliness<8, 32, 8>>;

struct liveliness_recorder final : plexus::io::observer
{
    std::vector<plexus::io::peer_liveliness_event> events;

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        events.push_back(ev);
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    std::size_t count(const plexus::node_id &id, plexus::io::liveliness_verdict verdict) const
    {
        return static_cast<std::size_t>(std::ranges::count_if(events, [&](const plexus::io::peer_liveliness_event &ev) { return ev.id == id && ev.verdict == verdict; }));
    }
};

template<typename Engine>
void announce_eight(basic_one_node<Engine> &node)
{
    for(std::uint8_t i = 1; i <= 8; ++i)
        node.eng.note_peer(make_id(i), ep_for("peer"));
    node.drain();
}

template<typename Engine>
void tick(basic_one_node<Engine> &node)
{
    manual_clock::advance(plexus::io::k_tick_granularity);
    node.drain();
}

constexpr auto alive = plexus::io::liveliness_verdict::alive;
constexpr auto lost  = plexus::io::liveliness_verdict::lost;

}

TEST_CASE("bounded engine: a ninth announced card is dropped and counted while the eight known peers stay reachable", "[graph][bounded]")
{
    manual_clock::reset();
    basic_one_node<tallied_engine> node;

    for(std::uint8_t i = 1; i <= 8; ++i)
        node.eng.note_peer(make_id(i), ep_for("peer"));
    node.drain();
    const std::size_t before = tallied_storage::tally;

    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    node.drain();

    REQUIRE(tallied_storage::tally == before + 1);
    REQUIRE_FALSE(node.eng.known().contains(make_id(9)));
    for(std::uint8_t i = 1; i <= 8; ++i)
        REQUIRE(node.eng.known().dial_target(make_id(i)) == ep_for("peer"));
}

TEST_CASE("bounded engine with a fixed liveliness arbiter of 8: a ninth announced card is refused and counted, not tracked", "[graph][bounded]")
{
    manual_clock::reset();
    basic_one_node<fixed_arbiter_engine> node;
    announce_eight(node);
    const std::size_t before = tallied_storage::tally;

    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    REQUIRE_NOTHROW(node.eng.note_peer(make_id(3), ep_for("peer")));
    tick(node);

    REQUIRE(tallied_storage::tally == before + 1);
    REQUIRE_FALSE(node.eng.known().contains(make_id(9)));
    REQUIRE(node.eng.known().dial_target(make_id(3)) == ep_for("peer"));
}

TEST_CASE("bounded engine: a refused card raises no liveliness verdict, and the same card is tracked once the table admits it", "[graph][bounded]")
{
    manual_clock::reset();
    liveliness_recorder recorder;
    basic_one_node<tallied_engine> node;
    node.eng.add_observer(recorder);
    announce_eight(node);

    node.eng.note_peer(make_id(9), ep_for("ninth"));
    node.eng.note_peer(make_id(3), ep_for("peer"));
    tick(node);

    REQUIRE(recorder.count(make_id(9), alive) == 0);
    for(std::uint8_t i = 1; i <= 8; ++i)
        REQUIRE(recorder.count(make_id(i), alive) == 1);

    node.eng.forget(make_id(1));
    node.eng.note_peer(make_id(9), ep_for("ninth"));
    tick(node);

    REQUIRE(recorder.count(make_id(1), lost) == 1);
    REQUIRE(recorder.count(make_id(9), alive) == 1);
    node.eng.remove_observer(recorder);
}

TEST_CASE("bounded engine with a fixed liveliness arbiter of 8: a goodbye frees the arbiter row for a card admitted the same turn", "[graph][bounded]")
{
    manual_clock::reset();
    liveliness_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node;
    node.eng.add_observer(recorder);
    announce_eight(node);

    node.eng.forget(make_id(1));
    CHECK(recorder.count(make_id(1), lost) == 0);
    node.drain();
    CHECK(recorder.count(make_id(1), lost) == 1);

    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    tick(node);

    REQUIRE(node.eng.known().contains(make_id(9)));
    REQUIRE(recorder.count(make_id(1), lost) == 1);
    REQUIRE(recorder.count(make_id(9), alive) == 1);
    node.eng.remove_observer(recorder);
}

TEST_CASE("bounded engine with a fixed liveliness arbiter of 8 under all_required: a goodbye leaves no arbiter row behind", "[graph][bounded]")
{
    manual_clock::reset();
    plexus::io::liveliness_options live;
    live.policy = plexus::io::combine::all_required;
    liveliness_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node(live);
    node.eng.add_observer(recorder);
    announce_eight(node);
    tick(node);

    node.eng.forget(make_id(1));
    tick(node);
    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    tick(node);

    REQUIRE(node.eng.known().contains(make_id(9)));
    REQUIRE(recorder.count(make_id(9), alive) == 1);
    node.eng.remove_observer(recorder);
}
