#include "test_bounded_card_refusal_common.h"

#include "plexus/io/liveliness_options.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <vector>
#include <cstdint>

using namespace bounded_card_refusal_fixture;

namespace {

constexpr std::chrono::milliseconds k_ttl{500};

enum class seen : std::uint8_t
{
    graph_changed,
    lost_edge
};

struct order_recorder final : plexus::io::observer
{
    std::vector<seen> order;

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        if(ev.verdict == lost)
            order.push_back(seen::lost_edge);
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    void on_graph_changed(std::uint64_t) override
    {
        order.push_back(seen::graph_changed);
    }
};

plexus::io::liveliness_options aging_options()
{
    plexus::io::liveliness_options live;
    live.policy        = plexus::io::combine::any_signal_alive;
    live.awareness_ttl = k_ttl;
    return live;
}

}

TEST_CASE("awareness withdrawal order: a goodbye posts the disappearance before the lost liveliness edge", "[graph][liveliness]")
{
    manual_clock::reset();
    order_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node(aging_options());
    node.eng.add_observer(recorder);
    node.eng.note_peer(make_id(1), ep_for("peer"));
    tick(node);
    recorder.order.clear();

    node.eng.forget(make_id(1));
    node.drain();

    REQUIRE(recorder.order == std::vector<seen>{seen::graph_changed, seen::lost_edge});
    node.eng.remove_observer(recorder);
}

TEST_CASE("awareness withdrawal order: an awareness expiry posts the disappearance before the lost liveliness edge", "[graph][liveliness]")
{
    manual_clock::reset();
    order_recorder recorder;
    basic_one_node<fixed_arbiter_engine> node(aging_options());
    node.eng.add_observer(recorder);
    node.eng.note_peer(make_id(1), ep_for("peer"));
    tick(node);
    recorder.order.clear();

    manual_clock::advance(k_ttl);
    tick(node);

    REQUIRE_FALSE(node.eng.known().contains(make_id(1)));
    REQUIRE(recorder.order == std::vector<seen>{seen::graph_changed, seen::lost_edge});
    node.eng.remove_observer(recorder);
}
