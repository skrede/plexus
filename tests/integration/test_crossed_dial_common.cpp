#include "test_crossed_dial_common.h"

#include "plexus/io/message_info.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace crossed_dial_fixture {

std::string hex_of(const node_id &id)
{
    constexpr std::string_view digits = "0123456789abcdef";
    std::string out;
    for(const std::byte b : id)
    {
        out.push_back(digits[std::to_integer<std::uint8_t>(b) >> 4]);
        out.push_back(digits[std::to_integer<std::uint8_t>(b) & 0x0f]);
    }
    return out;
}

std::vector<std::string> resolution_lines(const recording_logger &log)
{
    std::vector<std::string> out;
    for(const auto &line : log.lines)
        if(line.starts_with("plexus: crossed dial with "))
            out.push_back(line);
    return out;
}

std::size_t complete_sessions_for(engine &e, const node_id &proven)
{
    std::size_t n = 0;
    e.registry().for_each_connected([&](const node_id &, engine::session_type &s) { n += s.peer_identity() == proven ? 1 : 0; });
    return n;
}

std::optional<node_id> kept_slot(engine &e, const node_id &proven)
{
    std::vector<node_id> slots;
    e.registry().for_each_connected(
            [&](const node_id &id, engine::session_type &s)
            {
                if(s.peer_identity() == proven)
                    slots.push_back(id);
            });
    return slots.size() == 1 ? std::optional<node_id>{slots.front()} : std::nullopt;
}

std::int32_t verdicts_for(const recording_liveliness_observer &obs, const node_id &id, std::size_t from)
{
    std::int32_t n = 0;
    for(std::size_t i = from; i < obs.events.size(); ++i)
        n += obs.events[i].id == id ? 1 : 0;
    return n;
}

node_side::node_side(transport_t &transport, inproc_executor<manual_clock> &ex, std::uint8_t seed, const liveliness_options &live)
        : log()
        , edges()
        , received()
        , verdicts()
        , liveness()
        , e(transport, ex, make_cfg(seed), k_window, forever_cfg(), k_seed + seed, log, false, plexus::io::global_default_max_message_bytes, live)
{
    e.add_observer(edges);
    e.add_observer(verdicts);
    e.on_liveness([this](const plexus::io::liveness_event &ev) { liveness.push_back(ev); });
    e.on_message_route([this](std::string_view, std::span<const std::byte> data, const plexus::io::message_info &)
                       { received.emplace_back(reinterpret_cast<const char *>(data.data()), data.size()); });
}

crossed_net::crossed_net(const liveliness_options &live)
        : id_a(make_id(0xC3))
        , id_b(make_id(0x5A))
        , id_c(make_id(0x77))
        , ep_a{"inproc", "crossed-a"}
        , ep_b{"inproc", "crossed-b"}
        , ep_c{"inproc", "crossed-c"}
        , bus()
        , ex(bus)
        , transport_a(ex, bus)
        , transport_b(ex, bus)
        , transport_c(ex, bus)
        , a(transport_a, ex, 0xC3, live)
        , b(transport_b, ex, 0x5A, live)
        , c(transport_c, ex, 0x77, live)
{
    a.e.listen(ep_a);
    b.e.listen(ep_b);
    c.e.listen(ep_c);
}

void crossed_net::drive()
{
    ex.drain();
}

void crossed_net::advance_by(std::chrono::nanoseconds total)
{
    for(std::chrono::nanoseconds done{0}; done < total; done += k_step)
    {
        manual_clock::advance(k_step);
        drive();
    }
}

// newcomer_loses: a dials first and subscribes, then b dials a. incumbent_loses: b dials first and subscribes,
// then a dials b. The second dial waits one tick, so the first session has been judged alive before it is resolved.
void crossed_net::cross(crossing order, const plexus::io::subscriber_qos &qos)
{
    if(order != crossing::incumbent_loses)
        a.e.note_peer(id_b, ep_b);
    if(order != crossing::newcomer_loses)
        b.e.note_peer(id_a, ep_a);
    if(order == crossing::at_once)
    {
        a.e.reach(id_b);
        b.e.reach(id_a);
        return drive();
    }
    node_side &first  = order == crossing::newcomer_loses ? a : b;
    node_side &second = order == crossing::newcomer_loses ? b : a;
    first.e.subscribe(order == crossing::newcomer_loses ? id_b : id_a, k_topic, qos);
    advance_by(k_step);
    second.e.dial(order == crossing::newcomer_loses ? ep_a : ep_b);
    drive();
}

void crossed_net::publish_from(node_side &side)
{
    const std::string payload{"one"};
    side.e.publish(k_topic, std::as_bytes(std::span{payload}));
    drive();
}

}
