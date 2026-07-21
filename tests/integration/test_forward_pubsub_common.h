#ifndef HPP_GUARD_TESTS_INTEGRATION_FORWARD_PUBSUB_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_FORWARD_PUBSUB_COMMON_H

// A deterministic publisher -> relay -> subscriber pub/sub substrate over one inproc bus: a publisher
// leaf, a relay carrying the REAL forward splice, and a subscriber leaf with NO direct publisher<->
// subscriber session. The subscriber's demand propagates up to the relay, which subscribes the publisher
// upstream, so the publisher's plain publish reaches the relay and is re-originated as a forwarded
// envelope onto the subscriber, stamping the publisher's handshake-proven identity. The default `engine`
// threads null_forward_splice (why the splice was unit-driven via sp.refan directly), so the relay is
// spelled with the wants_refan()-capable forward_splice alias below.

#include "test_routing_engine_inproc_common.h"

#include "plexus/io/message_info.h"
#include "plexus/io/forward_options.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/io/detail/forward_splice.h"

#include "plexus/graph/topic_record.h"

#include "plexus/topic_qos.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace forward_pubsub_fixture {

namespace wire = plexus::wire;

using routing_inproc_fixture::engine;
using routing_inproc_fixture::manual_policy;
using routing_inproc_fixture::manual_clock;
using routing_inproc_fixture::transport_t;
using routing_inproc_fixture::make_cfg;
using routing_inproc_fixture::make_id;
using routing_inproc_fixture::forever_cfg;
using routing_inproc_fixture::k_long_timeout;
using routing_inproc_fixture::k_seed;
using routing_inproc_fixture::as_bytes;
using routing_inproc_fixture::to_string;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::io::endpoint;
using plexus::node_id;

// The real-splice relay engine: it carries the wants_refan()-capable forward_splice keyed on the inproc
// policy, so a relay both re-fans an admitted forwarded frame AND re-originates a directly-attached
// publisher's plain publish.
using relay_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, plexus::io::std_map_peer_storage, plexus::graph::std_map_topic_storage,
                                                plexus::io::default_liveliness_storage, plexus::graph::vector_graph_change_log, plexus::io::null_peer_report_emitter,
                                                plexus::io::forward_splice<manual_policy>>;

using pubsub_channel   = manual_policy::byte_channel_type;
using pubsub_forwarder = plexus::io::message_forwarder<manual_policy>;

struct delivery
{
    std::string fqn;
    std::string body;
    plexus::io::message_info info;
};

struct pubsub_line
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t t_pub{ex, bus};
    transport_t t_relay{ex, bus};
    transport_t t_sub{ex, bus};
    plexus::log::null_logger sink;

    engine pub;
    relay_engine relay;
    engine sub;

    node_id id_pub{make_id(0xB0)};
    node_id id_relay{make_id(0x2A)};
    node_id id_sub{make_id(0x5B)};
    endpoint ep_pub{"inproc", "pubsub-pub"};
    endpoint ep_relay{"inproc", "pubsub-relay"};
    endpoint ep_sub{"inproc", "pubsub-sub"};

    std::vector<delivery> sub_got;
    std::vector<delivery> pub_got;

    explicit pubsub_line(plexus::io::forward_options fwd = {})
            : pub(t_pub, ex, make_cfg(0xB0), k_long_timeout, forever_cfg(), k_seed, sink)
            , relay(t_relay, ex, make_cfg(0x2A), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, {}, {}, {}, fwd)
            , sub(t_sub, ex, make_cfg(0x5B), k_long_timeout, forever_cfg(), k_seed, sink)
    {
        sub.on_message_route([this](std::string_view fqn, std::span<const std::byte> b, const plexus::io::message_info &info)
                             { sub_got.push_back({std::string{fqn}, to_string(b), info}); });
        pub.on_message_route([this](std::string_view fqn, std::span<const std::byte> b, const plexus::io::message_info &info)
                             { pub_got.push_back({std::string{fqn}, to_string(b), info}); });
        pub.listen(ep_pub);
        relay.listen(ep_relay);
        sub.listen(ep_sub);
    }

    void drive()
    {
        ex.drain();
    }

    // The publisher declares the topic; the relay dials the publisher, or the publisher dials the relay
    // (the accepted-origin arm, whose slot key is provisional). Either way the publisher's declaration
    // folds onto the relay's topic table.
    void connect(std::string_view fqn, bool emit_source_identity, bool publisher_dials)
    {
        pub.messages().declare(fqn, plexus::topic_qos{}, std::nullopt, emit_source_identity);
        if(publisher_dials)
        {
            pub.note_peer(id_relay, ep_relay);
            pub.reach(id_relay);
        }
        else
        {
            relay.note_peer(id_pub, ep_pub);
            relay.reach(id_pub);
        }
        drive();
    }

    // The subscriber dials the relay and subscribes the topic through it; the relay folds the demand and
    // propagates an upstream subscribe onto the publisher's session.
    void subscribe_through_relay(std::string_view fqn)
    {
        sub.note_peer(id_relay, ep_relay);
        sub.subscribe(id_relay, fqn);
        drive();
    }

    void publish(std::string_view fqn, std::string_view body)
    {
        pub.publish(fqn, as_bytes(std::string{body}));
        drive();
    }

    bool no_direct_pub_sub_session() const
    {
        return !sub.is_connected(id_pub) && !pub.is_connected(id_sub);
    }
};

// A header-on unidirectional frame for a topic, captured off a producer's wire — the exact shape the
// relay re-originates inside a forwarded envelope.
inline std::vector<std::byte> capture_inner(std::string_view fqn, std::string_view body)
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    pubsub_channel subch{ex};
    pubsub_channel cap{ex};
    subch.connect_to(cap.local_endpoint());
    std::vector<std::byte> framed;
    cap.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    plexus::log::null_logger sink;
    pubsub_forwarder producer{sink};
    producer.declare(fqn, plexus::topic_qos{});
    producer.attach_for_fanout(pubsub_forwarder::peer{subch, "producer"}, fqn);
    ex.drain();
    producer.publish(fqn, as_bytes(std::string{body}));
    ex.drain();
    return framed;
}

inline int count_forwarded(const std::vector<std::vector<std::byte>> &frames)
{
    int n = 0;
    for(const auto &f : frames)
    {
        const auto hdr = wire::decode_header(f);
        if(hdr && hdr->type == wire::msg_type::forwarded)
            ++n;
    }
    return n;
}

}

#endif
