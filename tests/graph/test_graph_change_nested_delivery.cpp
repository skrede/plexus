#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using namespace plexus::testing::graph_change_fixture;
using plexus::graph::change_kind;
using plexus::graph::topic_role;

struct graph_event
{
    bool delta;
    std::uint64_t generation;
    change_kind kind;
};

// Toggles one local topic from inside its delta or coarse callbacks, so every round is a real change
// made while a wakeup is fanning. appended holds the generation each change it made reached.
struct feeding_graph_observer final : public plexus::io::observer
{
    feeding_graph_observer(engine &eng, plexus::inproc::inproc_executor<manual_clock> &ex, std::int32_t delta_rounds, std::int32_t edge_rounds)
            : eng(eng)
            , ex(ex)
            , declared(false)
            , delta_rounds(delta_rounds)
            , edge_rounds(edge_rounds)
            , deltas_seen(0)
            , marker_at(-1)
            , events()
            , appended()
    {
    }

    void on_graph_changed(std::uint64_t generation) override
    {
        events.push_back(graph_event{false, generation, change_kind::appeared});
        if(edge_rounds > 0)
        {
            --edge_rounds;
            toggle();
        }
    }

    void on_graph_delta(const plexus::graph::graph_change &change) override
    {
        events.push_back(graph_event{true, 0, change.kind});
        if(++deltas_seen == 1)
            ex.post([this] { marker_at = deltas_seen; });
        if(delta_rounds > 0)
        {
            --delta_rounds;
            toggle();
        }
    }

    bool observes_graph() const override
    {
        return true;
    }

    void toggle()
    {
        if(declared)
            eng.forget_local_topic("topic/loop", topic_role::publisher);
        else
            eng.note_local_topic("topic/loop", "", std::nullopt, topic_role::publisher);
        declared = !declared;
        appended.push_back(eng.graph_generation());
    }

    std::vector<change_kind> delta_kinds() const
    {
        std::vector<change_kind> kinds;
        for(const graph_event &e : events)
            if(e.delta)
                kinds.push_back(e.kind);
        return kinds;
    }

    // A delta is ahead of its edge when the last on_graph_changed before it carries a generation
    // older than the one its change reached.
    std::size_t deltas_ahead_of_their_edge() const
    {
        std::size_t ahead  = 0;
        std::size_t index  = 0;
        std::uint64_t edge = 0;
        for(const graph_event &e : events)
            if(!e.delta)
                edge = e.generation;
            else if(index < appended.size() && edge < appended[index++])
                ++ahead;
        return ahead;
    }

    engine &eng;
    plexus::inproc::inproc_executor<manual_clock> &ex;
    bool declared;
    std::int32_t delta_rounds;
    std::int32_t edge_rounds;
    std::int32_t deltas_seen;
    std::int32_t marker_at;
    std::vector<graph_event> events;
    std::vector<std::uint64_t> appended;
};

std::vector<change_kind> alternating_kinds(std::size_t count)
{
    std::vector<change_kind> kinds;
    kinds.reserve(count);
    for(std::size_t i = 0; i < count; ++i)
        kinds.push_back(i % 2 == 0 ? change_kind::appeared : change_kind::disappeared);
    return kinds;
}

TEST_CASE("graph change nested delivery: a self-feeding delta observer lets other posted work run between "
          "its rounds, and every round's delta arrives exactly once",
          "[graph][change][nested]")
{
    constexpr std::int32_t k_rounds = 1000;
    one_node net;
    feeding_graph_observer obs{net.eng, net.ex, k_rounds, 0};
    net.eng.add_observer(obs);

    const std::uint64_t before = net.eng.graph_generation();
    obs.toggle();
    net.drain();

    REQUIRE(obs.marker_at >= 1);
    REQUIRE(obs.marker_at < k_rounds + 1);
    REQUIRE(obs.deltas_seen == k_rounds + 1);
    REQUIRE(obs.appended.size() == static_cast<std::size_t>(k_rounds + 1));
    REQUIRE(net.eng.graph_generation() - before == obs.appended.size());
    REQUIRE(obs.delta_kinds() == alternating_kinds(obs.appended.size()));
    REQUIRE(obs.deltas_ahead_of_their_edge() == 0);

    net.drain();
    REQUIRE(obs.deltas_seen == k_rounds + 1);
}

TEST_CASE("graph change nested delivery: a delta emitted from inside on_graph_delta follows the coarse edge "
          "of the generation it created",
          "[graph][change][nested]")
{
    one_node net;
    feeding_graph_observer obs{net.eng, net.ex, 1, 0};
    net.eng.add_observer(obs);

    obs.toggle();
    net.drain();

    REQUIRE(obs.appended.size() == 2);
    REQUIRE(obs.delta_kinds() == alternating_kinds(2));
    REQUIRE(obs.deltas_ahead_of_their_edge() == 0);
    REQUIRE(obs.events.back().delta);
}

TEST_CASE("graph change nested delivery: a delta emitted from inside on_graph_changed follows the coarse "
          "edge of the generation it created",
          "[graph][change][nested]")
{
    one_node net;
    feeding_graph_observer obs{net.eng, net.ex, 0, 1};
    net.eng.add_observer(obs);

    obs.toggle();
    net.drain();

    REQUIRE(obs.appended.size() == 2);
    REQUIRE(obs.delta_kinds() == alternating_kinds(2));
    REQUIRE(obs.deltas_ahead_of_their_edge() == 0);
    REQUIRE(obs.events.back().delta);
}
