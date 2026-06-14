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
using session = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::io::k_rxo_field_reliability;
using plexus::io::k_rxo_field_durability;
using plexus::io::k_rxo_field_deadline;
using plexus::io::k_rxo_field_max_message_bytes;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr int k_loops = 50;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A two-node inproc link: the dialer (the SUBSCRIBER) and the accepted end (the
// PRODUCER) handshake through the transport's listen/dial rendezvous. The subscriber
// captures its match outcome via the two subscribe-outcome observables; the producer
// declares its topic before the subscriber demands it. Mirrors the settled peer_session
// bridge harness (channels deferred in unique_ptr, sessions in optional, declared after
// the bus so destruction unwinds the channels first).
struct link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    msg_forwarder sub_messages{};    // the subscriber's forwarder
    msg_forwarder prod_messages{};   // the producer's forwarder
    rpc_forwarder sub_procedures{ex, k_long_timeout};
    rpc_forwarder prod_procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> sub_ctx;
    plexus::io::peer_context<inproc_policy> prod_ctx;
    std::optional<session> subscriber;
    std::optional<session> producer;

    std::vector<std::string> received;                          // data delivered to the subscriber
    std::vector<subscribe_status> refusals;                     // on_subscribe_refused statuses
    std::vector<std::uint8_t> degraded;                         // on_subscribe_degraded bitmasks

    explicit link()
    {
        transport.on_accepted([this](std::unique_ptr<inproc_channel<>> ch) {
            prod_ctx.channel = std::move(ch);
            prod_ctx.node_name = "subscriber-node";
            prod_ctx.peer_id = make_cfg(0x02).self_id;   // the subscriber's node_id
            producer.emplace(prod_ctx, ex, make_cfg(0x01), k_long_timeout,
                             prod_messages, prod_procedures, true);
            producer->start();
        });
        transport.on_dialed([this](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &) {
            sub_ctx.channel = std::move(ch);
            sub_ctx.node_name = "producer-node";
            sub_ctx.peer_id = make_cfg(0x01).self_id;    // the producer's node_id
            subscriber.emplace(sub_ctx, ex, make_cfg(0x02), k_long_timeout,
                               sub_messages, sub_procedures, false);
            subscriber->on_message([this](std::string_view, std::span<const std::byte> d) {
                received.emplace_back(to_string(d));
            });
            subscriber->on_subscribe_refused([this](std::uint64_t, subscribe_status s) {
                refusals.push_back(s);
            });
            subscriber->on_subscribe_degraded([this](std::uint64_t, std::uint8_t bits) {
                degraded.push_back(bits);
            });
            subscriber->start();
        });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }

    bool complete() const { return subscriber->is_complete() && producer->is_complete(); }
};

}

TEST_CASE("rxo compatibility: a strict incompatible reliability pair is refused, a compatible pair communicates")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The incompatible leg: the producer offers best_effort, the strict subscriber
        // requests reliable -> refused with incompatible_qos, NO data delivered.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::best_effort});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requested_reliability_reliable = true,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.empty());   // refused: no fan-out entry, no data
        }
        // The compatible leg: the producer offers reliable, the same strict subscriber
        // is admitted and a publish DELIVERS, with no refusal/degraded fire.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::reliable});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requested_reliability_reliable = true,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.size() == 1);
            REQUIRE(l.received[0] == "payload");
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: the same incompatible reliability pair connects under permissive and surfaces the degraded field")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        link l;
        l.drive();
        REQUIRE(l.complete());
        l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::best_effort});
        // The SAME best_effort/reliable mismatch under permissive: connect + surface.
        l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                        .requested_reliability_reliable = true,
                                                        .rxo = rxo_mode::permissive});
        l.drive();
        REQUIRE(l.refusals.empty());
        REQUIRE(l.degraded.size() == 1);                                    // the observable FIRED
        REQUIRE(l.degraded[0] != 0);                                        // NON-EMPTY (non-silent)
        REQUIRE((l.degraded[0] & k_rxo_field_reliability) != 0);            // names the right field

        l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
        l.drive();
        REQUIRE(l.received.size() == 1);   // permissive: data flows
        REQUIRE(l.received[0] == "payload");
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a strict incompatible durability pair is refused and permissive surfaces it")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // A non-latching producer offers `none`; a durability::all request is incompatible.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = false});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
        }
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = false});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all,
                                                            .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE((l.degraded[0] & k_rxo_field_durability) != 0);
        }
        // A latching producer + the same request is admitted with no degraded fire.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = true});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all,
                                                            .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a pub max-message-bytes over the sub-requested ceiling refuses under strict and surfaces under permissive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The producer declares a 16 MiB per-message max; the subscriber requests a
        // 4 MiB ceiling — the publisher can emit larger than the subscriber accepts, the
        // incompatible direction. Strict refuses with the size bit named.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic",
                plexus::topic_qos{.max_message_bytes = 16u * 1024u * 1024u});
            l.subscriber->subscribe("topic",
                subscriber_qos{.durability_mode = durability::none,
                               .requested_max_message_bytes = 4u * 1024u * 1024u,
                               .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.empty());   // refused: no fan-out entry, no data
        }
        // The SAME pair under permissive connects, data flows, and the size bit surfaces.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic",
                plexus::topic_qos{.max_message_bytes = 16u * 1024u * 1024u});
            l.subscriber->subscribe("topic",
                subscriber_qos{.durability_mode = durability::none,
                               .requested_max_message_bytes = 4u * 1024u * 1024u,
                               .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE(l.degraded[0] != 0);
            REQUIRE((l.degraded[0] & k_rxo_field_max_message_bytes) != 0);

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.size() == 1);
            REQUIRE(l.received[0] == "payload");
        }
        // A subscriber requesting a ceiling AT/ABOVE the producer's max is admitted clean.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic",
                plexus::topic_qos{.max_message_bytes = 4u * 1024u * 1024u});
            l.subscriber->subscribe("topic",
                subscriber_qos{.durability_mode = durability::none,
                               .requested_max_message_bytes = 16u * 1024u * 1024u,
                               .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a requires source identity subscriber is refused against a non-offering producer regardless of mode")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        for(auto mode : {rxo_mode::permissive, rxo_mode::strict})
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            // The producer declares WITHOUT emit_source_identity (the 4th arg defaults false).
            l.prod_messages.declare("topic", plexus::topic_qos{});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requires_source_identity = true,
                                                            .rxo = mode});
            l.drive();
            // The always-hard floor: refused regardless of mode, and the degraded
            // observable must NOT fire (it is not a degradable field).
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::source_identity_incompatible);
            REQUIRE(l.degraded.empty());
        }
        // An offering producer admits the same requiring subscriber.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{}, std::nullopt, /*emit_source_identity=*/true);
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requires_source_identity = true,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a deadline or lease soft mismatch refuses under strict and surfaces under permissive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The producer offers a SLOWER deadline than the subscriber requests
        // (offered 200 > requested 100) — an incompatible offer.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 200});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requested_deadline_ns = 100,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
        }
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 200});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requested_deadline_ns = 100,
                                                            .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE((l.degraded[0] & k_rxo_field_deadline) != 0);
        }
        // An unset offered deadline (0) is always compatible — no fire in either mode.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 0});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none,
                                                            .requested_deadline_ns = 100,
                                                            .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}
