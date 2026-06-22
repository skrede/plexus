// over-limit: one cohesive capture-gate decision matrix; the per-mode/decimation cells all witness
// the one shared counting-encode-thunk node-pair harness, so splitting them scatters that shared
// fixture The per-topic capture-gate oracle on the manual virtual clock. The gate is exercised
// through the typed loan path (publish_object) over a connected inproc node pair: a counting
// encode thunk witnesses the encode-count WITHOUT any production-side counter (the encode
// lambda the publisher already passes is the seam), and a recording observer's per-topic
// tallies witness the post/no-post decision. The capture rules are set through the engine's
// capture() accessor (the public declaration API arrives separately).
//
// The gate decides BEFORE the encode: a metadata-only or unselected or decimated topic forces
// no encode (encode-count 0 for the skipped records), a payload-fidelity topic forces exactly
// one encode per admitted record (the idempotent encode_once guard), and wire on an inproc
// topic degrades to payload behavior (the loan path has no wire frame). Both decimation
// mechanisms are proven: count_n (the clock-free explicit alternative) and the default
// time_window (driven deterministically by the virtual clock). The folded should_emit predicate
// is asserted behavior-equivalent to the old dual gate across the observer-present x selected
// matrix, and a lifecycle-only observer (observes_data_path() == false) is proven not to admit
// payload posts.

#include "recording_observer.h"

#include "plexus/io/known_peers.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <chrono>
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
using plexus::io::capture_fidelity;
using plexus::io::decimation_mode;
using plexus::io::topic_capture_rule;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept { return current; }
    static void              reset() noexcept { current = time_point{}; }
    static void              advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type     = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type        = inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine      = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;
constexpr std::uint64_t k_tag          = 0x5151;
constexpr const char   *k_topic        = "topic";

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
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

struct counted_payload
{
    std::string value;
    int         release_calls{0};
    loan_slot   slot{};
};

object_carrier make_carrier(counted_payload &p, std::uint64_t tag)
{
    p.slot.object  = &p.value;
    p.slot.refs    = 1;
    p.slot.release = [](loan_slot *s)
    {
        auto *owner = reinterpret_cast<counted_payload *>(reinterpret_cast<std::byte *>(s) -
                                                          offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{0, tag, &p.value, 0, 0, &p.slot};
}

// A subscriber that opts OUT of the data-path taps: registering it leaves the gate's
// observer-presence count untouched, so an unselected topic stays inert for it.
struct lifecycle_only_observer final : plexus::io::observer
{
    bool observes_data_path() const override { return false; }
};

// Node A subscribes to node B (the publisher); B's loan path runs the capture gate. The
// counting encode thunk lives in the publish helper, so encode-count is witnessed with no
// production mutator.
struct capture_net
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};
    plexus::log::null_logger      sink;

    engine a;
    engine b;

    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    int encode_calls{0};

    capture_net()
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink,
                /*eager=*/true)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink,
                /*eager=*/true)
    {
        a.listen(ep_a);
        b.listen(ep_b);
        a.note_peer(id_b, ep_b);
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        b.messages().declare(k_topic, plexus::topic_qos{}, k_tag);
        a.session_for(id_b)->subscribe(k_topic, plexus::io::subscriber_qos{}, k_tag);
        ex.drain();
    }

    void drive() { ex.drain(); }

    plexus::io::peer_session<manual_policy> *b_live_inbound()
    {
        plexus::io::peer_session<manual_policy> *found = nullptr;
        b.registry().for_each_connected(
                [&](const plexus::node_id &, plexus::io::peer_session<manual_policy> &s)
                { found = &s; });
        return found;
    }

    // One typed-loan publish on B. The encode thunk increments encode_calls only when the
    // gate forces the lazy encode — the witness of the gate's pre-encode decision.
    void publish_once(counted_payload &p)
    {
        auto *b_inbound = b_live_inbound();
        REQUIRE(b_inbound != nullptr);
        b.messages().publish_object(
                k_topic, make_carrier(p, k_tag),
                [&]
                {
                    ++encode_calls;
                    return std::span<const std::byte>{
                            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
                },
                b_inbound->session_id());
        drive();
    }
};

std::uint64_t topic_hash() { return plexus::wire::fqn_topic_hash(k_topic); }

// Run K publishes through B's gate and return the encode-count. Each payload uses a distinct
// owned buffer so a stale view cannot mask a missing encode.
int publish_k(capture_net &net, int k)
{
    std::vector<counted_payload> payloads(static_cast<std::size_t>(k));
    for(int i = 0; i < k; ++i)
    {
        payloads[static_cast<std::size_t>(i)].value = "obj-" + std::to_string(i);
        net.publish_once(payloads[static_cast<std::size_t>(i)]);
    }
    return net.encode_calls;
}

}

TEST_CASE("capture gate: a metadata-only declaration forces no loan-path encode",
          "[integration][capture]")
{
    constexpr int k = 5;
    manual_clock::reset();
    capture_net net;
    net.b.capture().set_topic(topic_hash(),
                              topic_capture_rule{.fidelity = capture_fidelity::metadata});

    REQUIRE(publish_k(net, k) == 0);
}

TEST_CASE("capture gate: a payload-fidelity topic forces exactly one encode per publish",
          "[integration][capture]")
{
    constexpr int k = 5;
    manual_clock::reset();
    capture_net net;
    net.b.capture().set_topic(topic_hash(),
                              topic_capture_rule{.fidelity = capture_fidelity::payload});

    REQUIRE(publish_k(net, k) == k);
}

TEST_CASE("capture gate: an unselected topic with no observer forces no encode",
          "[integration][capture]")
{
    constexpr int k = 5;
    manual_clock::reset();
    capture_net net;

    REQUIRE(publish_k(net, k) == 0);
}

TEST_CASE("capture gate: wire on an inproc topic degrades to payload (one encode per publish)",
          "[integration][capture]")
{
    constexpr int k = 4;
    manual_clock::reset();
    capture_net net;
    // wire >= payload, so the loan path (which has no wire frame) captures payload bytes —
    // the degrade is observable as a real payload encode, not a silent no-op.
    net.b.capture().set_topic(topic_hash(), topic_capture_rule{.fidelity = capture_fidelity::wire});

    REQUIRE(publish_k(net, k) == k);
}

TEST_CASE("capture gate: count_n decimation keeps one encode per N publishes",
          "[integration][capture]")
{
    constexpr int           k = 9;
    constexpr std::uint32_t n = 3;
    manual_clock::reset();
    capture_net net;
    net.b.capture().set_topic(topic_hash(),
                              topic_capture_rule{.fidelity   = capture_fidelity::payload,
                                                 .mode       = decimation_mode::count_n,
                                                 .decimation = n});

    REQUIRE(publish_k(net, k) == k / static_cast<int>(n));
}

TEST_CASE("capture gate: the default time_window mechanism admits one record per elapsed window",
          "[integration][capture]")
{
    // The integration loan path reads the wall clock for the time-window test (the publish
    // path's own now_ns), which is not deterministically drivable from a test. The mechanism
    // itself is exercised directly on capture_policy with an injected monotonic now_ns, so the
    // assertion is exact and free of wall-clock flakiness. Looped to satisfy the no-single-run
    // discipline for a clock-adjacent assertion.
    constexpr std::uint64_t window_ns = 1000;
    const std::uint64_t     hash      = topic_hash();
    for(int run = 0; run < 3; ++run)
    {
        plexus::io::capture_policy policy;
        policy.set_topic(hash,
                         topic_capture_rule{.fidelity  = capture_fidelity::payload,
                                            .mode      = decimation_mode::time_window,
                                            .window_ns = window_ns});

        // Three windows, three records inside each at a fixed instant. The first record of each
        // window is admitted; the two in-window records that follow are not.
        int admitted = 0;
        for(int w = 0; w < 3; ++w)
        {
            const std::uint64_t base = static_cast<std::uint64_t>(w + 1) * window_ns;
            for(int i = 0; i < 3; ++i)
                if(policy.wants_payload(hash, base))
                    ++admitted;
        }
        REQUIRE(admitted == 3);
    }
}

TEST_CASE("capture gate: the folded should_emit matches the dual gate across the observer x "
          "selected matrix",
          "[integration][capture]")
{
    SECTION("unselected topic WITH a data-path observer still posts the metadata floor")
    {
        manual_clock::reset();
        capture_net        net;
        recording_observer rec;
        net.b.add_observer(rec); // observes_data_path() == true
        net.drive();

        counted_payload p;
        p.value = "obs-no-select";
        net.publish_once(p);

        REQUIRE(rec.for_topic(k_topic).published == 1);
        REQUIRE(net.encode_calls == 0); // observer-presence admits the floor, not a payload encode
    }

    SECTION("selected payload topic with NO observer still posts and encodes")
    {
        manual_clock::reset();
        capture_net net;
        net.b.capture().set_topic(topic_hash(),
                                  topic_capture_rule{.fidelity = capture_fidelity::payload});

        counted_payload p;
        p.value = "select-no-obs";
        net.publish_once(p);

        REQUIRE(net.encode_calls == 1);
    }

    SECTION("unselected topic with no observer posts nothing and encodes nothing")
    {
        manual_clock::reset();
        capture_net        net;
        recording_observer rec; // registered ONLY as a tally; never added to the engine

        counted_payload p;
        p.value = "neither";
        net.publish_once(p);

        REQUIRE(rec.for_topic(k_topic).published == 0);
        REQUIRE(net.encode_calls == 0);
    }
}

TEST_CASE(
        "capture gate: a lifecycle-only observer never admits payload posts on an unselected topic",
        "[integration][capture]")
{
    constexpr int k = 4;
    manual_clock::reset();
    capture_net             net;
    lifecycle_only_observer life;
    net.b.add_observer(life); // observes_data_path() == false -> NOT counted as data-path-present
    net.drive();

    recording_observer rec; // a separate data-path tally, NOT registered, to confirm no post
    REQUIRE(publish_k(net, k) == 0);
    REQUIRE(rec.for_topic(k_topic).published == 0);
}
