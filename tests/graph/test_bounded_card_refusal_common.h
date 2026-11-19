#ifndef HPP_GUARD_PLEXUS_TESTS_GRAPH_BOUNDED_CARD_REFUSAL_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_GRAPH_BOUNDED_CARD_REFUSAL_COMMON_H

#include "support/graph_change_inproc.h"

#include "plexus/target_profile.h"

#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/peer_liveliness_event.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace bounded_card_refusal_fixture {

using plexus::testing::graph_change_fixture::ep_for;
using plexus::testing::graph_change_fixture::make_id;
using plexus::testing::graph_change_fixture::transport_t;
using plexus::testing::graph_change_fixture::manual_clock;
using plexus::testing::graph_change_fixture::manual_policy;
using plexus::testing::graph_change_fixture::basic_one_node;

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

#endif
