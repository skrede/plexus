#include "test_bounded_card_refusal_common.h"

#include "plexus/io/liveliness_options.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

using namespace bounded_card_refusal_fixture;

namespace {

constexpr std::chrono::milliseconds k_ttl{500};

plexus::io::liveliness_options aging_options(plexus::io::combine policy)
{
    plexus::io::liveliness_options live;
    live.policy        = policy;
    live.awareness_ttl = k_ttl;
    return live;
}

void age_out(basic_one_node<fixed_arbiter_engine> &node)
{
    manual_clock::advance(k_ttl);
    tick(node);
}

}

TEST_CASE("bounded engine with a fixed liveliness arbiter of 8 under all_required: an awareness expiry leaves no arbiter row behind", "[graph][bounded]")
{
    manual_clock::reset();
    liveliness_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node(aging_options(plexus::io::combine::all_required));
    node.eng.add_observer(recorder);
    announce_eight(node);
    tick(node);
    REQUIRE(recorder.count(make_id(1), alive) == 1);

    age_out(node);
    REQUIRE_FALSE(node.eng.known().contains(make_id(1)));
    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    tick(node);

    REQUIRE(node.eng.known().contains(make_id(9)));
    REQUIRE(recorder.count(make_id(9), alive) == 1);
    node.eng.remove_observer(recorder);
}

TEST_CASE("bounded engine with a fixed liveliness arbiter of 8 under any_signal_alive: an awareness expiry posts the lost edge and frees the row", "[graph][bounded]")
{
    manual_clock::reset();
    liveliness_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node(aging_options(plexus::io::combine::any_signal_alive));
    node.eng.add_observer(recorder);
    announce_eight(node);
    tick(node);

    age_out(node);
    REQUIRE_FALSE(node.eng.known().contains(make_id(1)));
    REQUIRE(recorder.count(make_id(1), lost) == 1);
    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    tick(node);

    REQUIRE(node.eng.known().contains(make_id(9)));
    REQUIRE(recorder.count(make_id(1), lost) == 1);
    REQUIRE(recorder.count(make_id(9), alive) == 1);
    node.eng.remove_observer(recorder);
}
