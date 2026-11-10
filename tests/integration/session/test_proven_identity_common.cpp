#include "test_proven_identity_common.h"

#include "plexus/io/message_info.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstddef>
#include <optional>
#include <algorithm>

namespace proven_identity_fixture {

// An inbound slot id carries its sequence number in the low bytes only, and node_id{} has no byte set at all.
bool provisional(const node_id &id)
{
    return std::all_of(id.begin(), id.begin() + 8, [](std::byte b) { return b == std::byte{0}; });
}

handshake_fsm_config older_cfg(std::uint8_t id_seed)
{
    handshake_fsm_config cfg     = make_cfg(id_seed);
    cfg.version_major            = 0;
    cfg.compatible_version_major = 0;
    return cfg;
}

id_recorder::id_recorder()
        : ids()
        , verdicts()
        , rejected()
        , rejected_names()
{
}

void id_recorder::on_peer_connected(const node_id &id, std::string_view, plexus::io::peer_kind)
{
    ids.push_back(id);
}

void id_recorder::on_peer_disconnected(const node_id &id, std::string_view, plexus::io::peer_kind)
{
    ids.push_back(id);
}

void id_recorder::on_peer_reconnected(const node_id &id, std::string_view, plexus::io::peer_kind)
{
    ids.push_back(id);
}

void id_recorder::on_peer_ready(const node_id &id, std::string_view, plexus::io::peer_kind)
{
    ids.push_back(id);
}

void id_recorder::on_peer_rejected(const node_id &id, std::string_view name, plexus::io::handshake_outcome)
{
    rejected.push_back(id);
    rejected_names.emplace_back(name);
}

void id_recorder::on_peer_liveliness(const peer_liveliness_event &ev)
{
    ids.push_back(ev.id);
    verdicts.push_back(ev.id);
}

void id_recorder::on_drop(const plexus::io::detail::drop_event &ev)
{
    ids.push_back(ev.peer);
}

bool id_recorder::observes_liveliness() const
{
    return true;
}

proven_net::proven_net()
        : id_a(make_id(0xA1))
        , id_b(make_id(0xB2))
        , ep_a{"inproc", "proven-a"}
        , ep_b{"inproc", "proven-b"}
        , bus()
        , ex(bus)
        , transport_a(ex, bus)
        , transport_b(ex, bus)
        , sink()
        , seen()
        , sources()
        , a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false)
        , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false)
{
    a.listen(ep_a);
    b.listen(ep_b);
    b.add_observer(seen);
    b.on_message_route([this](std::string_view, std::span<const std::byte>, const plexus::io::message_info &mi)
                       { sources.push_back(mi.source_identity ? mi.source_identity->node_id() : node_id{}); });
    a.messages().declare(k_topic, plexus::topic_qos{}, std::nullopt, true);
}

void proven_net::advance_by(std::chrono::nanoseconds total)
{
    for(std::chrono::nanoseconds done{0}; done < total; done += k_step)
    {
        manual_clock::advance(k_step);
        ex.drain();
    }
}

void proven_net::connect()
{
    a.note_peer(id_b, ep_b);
    a.reach(id_b);
    ex.drain();
}

void proven_net::publish()
{
    const std::string payload{"one"};
    a.publish(k_topic, std::as_bytes(std::span{payload}));
    ex.drain();
}

engine::session_type *proven_net::accepted_from_a()
{
    engine::session_type *found = nullptr;
    b.registry().for_each_connected(
            [&](const node_id &, engine::session_type &s)
            {
                if(s.peer_identity() == id_a)
                    found = &s;
            });
    return found;
}

void proven_net::drop_and_redial()
{
    engine::session_type *s = accepted_from_a();
    REQUIRE(s != nullptr);
    s->tear_down();
    ex.drain();
    advance_by(k_redial);
}

}
