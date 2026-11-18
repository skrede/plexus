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
