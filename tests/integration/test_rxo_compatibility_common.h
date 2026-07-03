#ifndef HPP_GUARD_TESTS_INTEGRATION_RXO_COMPATIBILITY_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_RXO_COMPATIBILITY_COMMON_H

// The end-to-end request-vs-offered compatibility hard gate over the inproc
// rendezvous: a subscriber (the dialer/requester) demands a producer's (the accepted
// responder's) topic carrying its OWN requested QoS + chosen rxo_mode; the producer
// runs the RxO check at the fan-out gate and replies the outcome on the existing
// subscribe_response seam, surfaced subscriber-side via on_subscribe_refused /
// on_subscribe_degraded. Proven behaviorally per soft field: a compatible pair
// COMMUNICATES; a strict incompatible pair is REFUSED (the reason surfaced, NO data on
// a subsequent publish); the SAME pair under permissive CONNECTS, data flows, AND the
// degraded-field set is surfaced NON-EMPTY naming the right field (the non-silent
// guarantee); requires_source_identity refuses against a non-offering producer under
// BOTH modes (the always-hard floor). Each behavioral leg looped for reproducibility.

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::durability;
using plexus::io::reliability;
using plexus::io::rxo_mode;
using plexus::io::subscriber_qos;
using plexus::io::handshake_fsm_config;
using plexus::wire::subscribe_status;
using session       = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::io::k_rxo_field_reliability;
using plexus::io::k_rxo_field_durability;
using plexus::io::k_rxo_field_deadline;
using plexus::io::k_rxo_field_max_message_bytes;

namespace rxo_fixture {

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr int k_loops         = 50;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A two-node inproc link: the dialer (the SUBSCRIBER) and the accepted end (the
// PRODUCER) handshake through the transport's listen/dial rendezvous. The subscriber
// captures its match outcome via the two subscribe-outcome observables; the producer
// declares its topic before the subscriber demands it. Mirrors the settled peer_session
// bridge harness (channels deferred in unique_ptr, sessions in optional, declared after
// the bus so destruction unwinds the channels first).
struct session_link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    plexus::log::null_logger sink;
    msg_forwarder sub_messages{sink};  // the subscriber's forwarder
    msg_forwarder prod_messages{sink}; // the producer's forwarder
    rpc_forwarder sub_procedures{ex, k_long_timeout, sink};
    rpc_forwarder prod_procedures{ex, k_long_timeout, sink};

    plexus::io::peer_context<inproc_policy> sub_ctx;
    plexus::io::peer_context<inproc_policy> prod_ctx;
    std::optional<session> subscriber;
    std::optional<session> producer;

    std::vector<std::string> received;      // data delivered to the subscriber
    std::vector<subscribe_status> refusals; // on_subscribe_refused statuses
    std::vector<std::uint8_t> degraded;     // on_subscribe_degraded bitmasks

    explicit session_link()
    {
        transport.on_accepted(
                [this](std::unique_ptr<inproc_channel<>> ch)
                {
                    prod_ctx.channel   = std::move(ch);
                    prod_ctx.node_name = "subscriber-node";
                    prod_ctx.peer_id   = make_cfg(0x02).self_id; // the subscriber's node_id
                    producer.emplace(prod_ctx, ex, make_cfg(0x01), k_long_timeout, prod_messages, prod_procedures, true, sink);
                    producer->start();
                });
        transport.on_dialed(
                [this](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
                {
                    sub_ctx.channel   = std::move(ch);
                    sub_ctx.node_name = "producer-node";
                    sub_ctx.peer_id   = make_cfg(0x01).self_id; // the producer's node_id
                    subscriber.emplace(sub_ctx, ex, make_cfg(0x02), k_long_timeout, sub_messages, sub_procedures, false, sink);
                    subscriber->on_message([this](std::string_view, std::span<const std::byte> d) { received.emplace_back(to_string(d)); });
                    subscriber->on_subscribe_refused([this](std::uint64_t, subscribe_status s) { refusals.push_back(s); });
                    subscriber->on_subscribe_degraded([this](std::uint64_t, std::uint8_t bits) { degraded.push_back(bits); });
                    subscriber->start();
                });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive()
    {
        ex.drain();
    }

    bool complete() const
    {
        return subscriber->is_complete() && producer->is_complete();
    }
};

}

#endif
