// The process-tier object-route oracle on the manual virtual clock. An object
// published on one engine's forwarder (publish_object) reaches the peer engine's
// object route in the SAME process with zero serialization: the route observes the
// SAME slot object pointer the publisher handed off (the address-identity witness at
// the engine seam). The route is the node-shared on_object_route threaded through the
// session_build_context, so a forced reconnect rebuild preserves it with no
// re-install. Every delivered reference is released, so slot refs return to zero.
// Deterministic inproc Policy on the manual clock — no backend, no socket.

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::loan_slot;
using plexus::io::object_carrier;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type = inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn) { ex.post(std::move(fn)); }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;
constexpr std::uint64_t k_tag = 0x5151;

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

// A producer-side object slot with a counting release. value is the "live object"
// the route receives by address; release_calls increments when the slot drops to zero.
struct counted_payload
{
    std::string value;
    int release_calls{0};
    loan_slot slot{};
};

object_carrier make_carrier(counted_payload &p, std::uint64_t tag)
{
    p.slot.object = &p.value;
    p.slot.refs = 1;   // publish_object's caller owns one reference on entry
    p.slot.release = [](loan_slot *s) {
        auto *owner = reinterpret_cast<counted_payload *>(
            reinterpret_cast<std::byte *>(s) - offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{0, tag, &p.value, 0, 0, &p.slot};
}

std::span<const std::byte> encode_value(const std::string &v)
{
    return {reinterpret_cast<const std::byte *>(v.data()), v.size()};
}

// Node A (the DIALER, hence the slot that REBUILDS on reconnect) subscribing to node
// B (the publisher). Member ORDER: bus/executor/transports BEFORE the engines so
// destruction unwinds the engines' channels before the bus.
struct object_net
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};

    engine a;
    engine b;

    plexus::node_id id_b{make_id(0xB2)};
    endpoint ep_a{"inproc", "node-a"};
    endpoint ep_b{"inproc", "node-b"};

    object_net()
        : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, /*eager=*/true)
        , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, /*eager=*/true)
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void drive() { ex.drain(); }

    plexus::io::peer_session<manual_policy> *b_live_inbound()
    {
        plexus::io::peer_session<manual_policy> *found = nullptr;
        b.registry().for_each_connected([&](const plexus::node_id &,
                                            plexus::io::peer_session<manual_policy> &s) {
            found = &s;
        });
        return found;
    }

    void connect_and_wire(const std::string &topic)
    {
        a.note_peer(id_b, ep_b);
        drive();
        REQUIRE(a.is_connected(id_b));
        rewire_fanout(topic);
    }

    // (Re)establish the typed demand for the live A->B pair. B declares the topic type
    // BEFORE A's subscribe arrives so the match admits the fast path; A's counted
    // demand-subscribe carries A's type_id over the wire — B's session reacts with
    // attach_for_fanout, registering A's inbound slot with the SAME tag (so eligibility
    // passes) AND recording the topic_hash->fqn mapping A resolves inbound objects by.
    // The wire-driven attach is the ONLY producer-side registration: a second explicit
    // attach_for_fanout would double the refcount. Called after a reconnect rebuild too.
    void rewire_fanout(const std::string &topic)
    {
        b.messages().declare(topic, plexus::topic_qos{}, k_tag);
        a.session_for(id_b)->subscribe(topic, plexus::io::subscriber_qos{}, k_tag);
        drive();
    }

    void publish_object(const std::string &topic, counted_payload &p)
    {
        auto *b_inbound = b_live_inbound();
        REQUIRE(b_inbound != nullptr);
        b.messages().publish_object(topic, make_carrier(p, k_tag),
                                    [&] { return encode_value(p.value); },
                                    b_inbound->session_id());
        drive();
    }
};

}

TEST_CASE("object route: an object published on B reaches A's object route with the same slot address",
          "[integration][routing][object][inproc]")
{
    constexpr int k_iterations = 50;
    const std::string topic = "topic";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        object_net net;

        std::vector<std::string> fqns;
        std::vector<const void *> object_addrs;
        std::vector<std::uint64_t> tags;
        std::vector<std::uint64_t> seqs;
        net.a.on_object_route([&](std::string_view fqn, const object_carrier &c) {
            fqns.emplace_back(fqn);
            object_addrs.push_back(c.slot->object);
            tags.push_back(c.type_tag);
            seqs.push_back(c.sequence);
            // The route is a read-only delivery (like on_message_route): the carrier is
            // valid for the callback duration; the session owns the single release.
        });

        net.connect_and_wire(topic);

        counted_payload p;
        p.value = "zero-copy-object";
        const void *handed_off = &p.value;
        net.publish_object(topic, p);

        REQUIRE(fqns.size() == 1);
        REQUIRE(fqns.front() == topic);
        REQUIRE(tags.front() == k_tag);
        // The address-identity witness: A's route saw the SAME object B handed off,
        // with no copy/serialization between them.
        REQUIRE(object_addrs.front() == handed_off);
        // Slot reference balance: caller + bus references both released.
        REQUIRE(p.slot.refs == 0u);
        REQUIRE(p.release_calls == 1);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("object route: an unresolvable topic_hash releases and never delivers",
          "[integration][routing][object][inproc]")
{
    manual_clock::reset();
    object_net net;

    int fires = 0;
    net.a.on_object_route([&](std::string_view, const object_carrier &) {
        ++fires;
    });

    net.connect_and_wire("topic");

    // Send an object on a topic A never recorded: the carrier's topic_hash resolves to
    // no fqn on A, so A drops + releases without firing the route. B publishes by
    // sending the carrier directly over the live channel's object lane.
    auto *b_inbound = net.b_live_inbound();
    REQUIRE(b_inbound != nullptr);
    counted_payload p;
    p.value = "unresolved";
    object_carrier c = make_carrier(p, k_tag);   // caller owns one reference (refs == 1)
    c.topic_hash = 0xDEADBEEFu;                   // a hash A has no fqn mapping for
    b_inbound->msg_peer().channel.send_object(c); // the bus addrefs; the delivery releases it again
    plexus::io::release(c);       // release the caller's own reference (as publish_object would)
    net.drive();

    REQUIRE(fires == 0);
    REQUIRE(p.slot.refs == 0u);
    REQUIRE(p.release_calls == 1);
}

TEST_CASE("object route: the route survives a forced reconnect rebuild without re-install (looped)",
          "[integration][routing][object][inproc]")
{
    constexpr int k_reconnects = 6;
    const std::string topic = "topic";
    manual_clock::reset();
    object_net net;

    // Install the shared object route ONCE, before any session exists.
    std::vector<const void *> addrs;
    net.a.on_object_route([&](std::string_view, const object_carrier &c) {
        addrs.push_back(c.slot->object);
    });

    net.connect_and_wire(topic);

    counted_payload first;
    first.value = "before-reconnect";
    net.publish_object(topic, first);
    REQUIRE(addrs.size() == 1);
    REQUIRE(addrs.back() == &first.value);
    REQUIRE(first.release_calls == 1);

    std::vector<counted_payload> payloads(k_reconnects);
    for(int r = 0; r < k_reconnects; ++r)
    {
        const auto epoch_before = net.a.session_for(net.id_b)->session_id();

        net.a.registry().driver_for(net.id_b).on_channel_dropped();
        manual_clock::advance(std::chrono::milliseconds(10001));
        net.ex.drain();

        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != epoch_before);

        net.rewire_fanout(topic);
        payloads[r].value = "after-reconnect-" + std::to_string(r);
        net.publish_object(topic, payloads[r]);

        REQUIRE(addrs.size() == static_cast<std::size_t>(r + 2));
        REQUIRE(addrs.back() == &payloads[r].value);
        REQUIRE(payloads[r].release_calls == 1);
    }
}
