#ifndef HPP_GUARD_BENCHMARKS_RELAY_RELAY_TOPOLOGY_H
#define HPP_GUARD_BENCHMARKS_RELAY_RELAY_TOPOLOGY_H

#include "plexus/io/routing_engine.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/detail/forward_splice.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/topic_qos.h"
#include "plexus/node_id.h"

#include <span>
#include <string>
#include <cstdint>
#include <chrono>

namespace relay_bench {

using bench_clock = std::chrono::steady_clock;

using policy_type    = plexus::inproc::inproc_policy;
using transport_type = plexus::inproc::inproc_transport<>;
using engine         = plexus::io::routing_engine<policy_type, transport_type>;
using relay_engine   = plexus::io::routing_engine<policy_type, transport_type, bench_clock, plexus::io::std_map_peer_storage,
                                                  plexus::graph::std_map_topic_storage, plexus::io::default_liveliness_storage,
                                                  plexus::graph::vector_graph_change_log, plexus::io::null_peer_report_emitter,
                                                  plexus::io::forward_splice<policy_type>>;

constexpr auto          k_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed    = 0xC0FFEEu;

struct sample
{
    std::uint64_t delivered{};
    std::uint64_t wall_ns{};
};

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::io::handshake_fsm_config make_cfg(std::uint8_t seed)
{
    return plexus::io::handshake_fsm_config{
        .self_id = make_id(seed), .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

// Slots sized past the largest swept payload plus its forwarded envelope so the splice never
// oversize-drops; the sweep measures forwarding cost, not the default 2 KiB slot cap.
inline plexus::io::forward_options bench_forward_opts()
{
    plexus::io::forward_options opts;
    opts.splice_slot_bytes = 64u * 1024u;
    return opts;
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// One publish + drain-to-quiescence per message, timed on the real steady clock: the inproc path is
// synchronous, so the per-message wall-time is the in-process delivery cost, and the relayed minus
// direct difference is the cost of the extra forward-splice hop.
template<typename Line>
sample time_publishes(Line &line, const std::string &fqn, std::size_t payload, std::uint64_t messages)
{
    const std::string body(payload, 'x');
    line.got  = 0;
    const auto t0 = bench_clock::now();
    for(std::uint64_t i = 0; i < messages; ++i)
    {
        line.pub.publish(fqn, as_bytes(body));
        line.ex.drain();
    }
    const auto t1 = bench_clock::now();
    return {line.got, static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count())};
}

struct direct_line
{
    plexus::inproc::inproc_bus<>      bus;
    plexus::inproc::inproc_executor<> ex{bus};
    transport_type                    t_pub{ex, bus};
    transport_type                    t_sub{ex, bus};
    plexus::log::null_logger          sink;
    engine                            pub;
    engine                            sub;
    plexus::node_id                   id_pub{make_id(0xB0)};
    plexus::io::endpoint              ep_pub{"inproc", "relay-bench-direct-pub"};
    plexus::io::endpoint              ep_sub{"inproc", "relay-bench-direct-sub"};
    std::uint64_t                     got{};

    direct_line()
            : pub(t_pub, ex, make_cfg(0xB0), k_timeout, forever_cfg(), k_seed, sink)
            , sub(t_sub, ex, make_cfg(0x5B), k_timeout, forever_cfg(), k_seed, sink)
    {
        sub.on_message_route([this](std::string_view, std::span<const std::byte>, const plexus::io::message_info &) { ++got; });
        pub.listen(ep_pub);
        sub.listen(ep_sub);
    }

    void connect(std::string_view fqn)
    {
        pub.messages().declare(fqn, plexus::topic_qos{});
        sub.note_peer(id_pub, ep_pub);
        sub.subscribe(id_pub, fqn);
        for(int i = 0; i < 1000 && !sub.is_connected(id_pub); ++i)
            ex.drain();
        ex.drain();
    }
};

struct relay_line
{
    plexus::inproc::inproc_bus<>      bus;
    plexus::inproc::inproc_executor<> ex{bus};
    transport_type                    t_pub{ex, bus};
    transport_type                    t_relay{ex, bus};
    transport_type                    t_sub{ex, bus};
    plexus::log::null_logger          sink;
    engine                            pub;
    relay_engine                      relay;
    engine                            sub;
    plexus::node_id                   id_pub{make_id(0xB0)};
    plexus::node_id                   id_relay{make_id(0x2A)};
    plexus::io::endpoint              ep_pub{"inproc", "relay-bench-pub"};
    plexus::io::endpoint              ep_relay{"inproc", "relay-bench-relay"};
    plexus::io::endpoint              ep_sub{"inproc", "relay-bench-sub"};
    std::uint64_t                     got{};

    relay_line()
            : pub(t_pub, ex, make_cfg(0xB0), k_timeout, forever_cfg(), k_seed, sink)
            , relay(t_relay, ex, make_cfg(0x2A), k_timeout, forever_cfg(), k_seed, sink, false,
                    plexus::io::global_default_max_message_bytes, {}, {}, {}, bench_forward_opts())
            , sub(t_sub, ex, make_cfg(0x5B), k_timeout, forever_cfg(), k_seed, sink)
    {
        sub.on_message_route([this](std::string_view, std::span<const std::byte>, const plexus::io::message_info &) { ++got; });
        pub.listen(ep_pub);
        relay.listen(ep_relay);
        sub.listen(ep_sub);
    }

    void connect(std::string_view fqn)
    {
        pub.messages().declare(fqn, plexus::topic_qos{});
        relay.note_peer(id_pub, ep_pub);
        relay.reach(id_pub);
        sub.note_peer(id_relay, ep_relay);
        sub.subscribe(id_relay, fqn);
        for(int i = 0; i < 1000 && !sub.is_connected(id_relay); ++i)
            ex.drain();
        ex.drain();
    }
};

}

#endif
