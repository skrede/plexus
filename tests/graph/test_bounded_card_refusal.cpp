#include "test_bounded_card_refusal_common.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

using namespace bounded_card_refusal_fixture;

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
