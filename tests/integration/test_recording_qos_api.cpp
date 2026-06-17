// The public recording-QoS declaration oracle: it drives the capture gate through the
// PUBLIC node facade alone (node / node_options / publisher / subscriber), touching no
// internal type. A counting codec witnesses the gate's pre-encode decision — the codec's
// encode runs ONLY when the gate forces the lazy payload encode, so the encode-count is the
// public-API witness of capture without any production-side mutator. Sections prove: the off
// default ships zero capture, a node-level payload default fires the encode per publish, and
// a per-topic publisher override raises capture for one topic above an off node default.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct sample
{
    std::uint32_t value{};
};

// A codec whose encode bumps a shared counter, so the encode-count is observable through the
// PUBLIC publish path. The gate decides BEFORE the encode, so a non-zero count witnesses that
// the capture declaration selected this topic for payload.
struct counting_codec
{
    using value_type = sample;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {0xABCD1234u, "sample"}; }
};

static_assert(plexus::typed_codec<counting_codec>);

using typed_publisher = plexus::publisher<counting_codec>;
using typed_subscriber = plexus::subscriber<counting_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options base_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0DEu;
    opts.dial_eagerly = eager;
    return opts;
}

// A two-node inproc net: the producer node b publishes; the consumer node a subscribes so the
// loan path runs the producer's capture gate. The producer's node_options carry the recording
// declaration under test.
struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    plexus::node_options opts_a{base_opts(/*eager=*/true)};
    plexus::node_options opts_b;

    inproc_node a;
    inproc_node b;

    explicit net(const plexus::node_options &producer_opts)
        : opts_b(producer_opts)
        , a(ex, disc, id_a, ta, opts_a)
        , b(ex, disc, id_b, tb, opts_b)
    {
    }

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
    }
};

// Subscribe a on topic, publish K loans on b through topic_pub, return the encode-count.
int publish_k(typed_publisher &pub, net &n, int k)
{
    for(int i = 0; i < k; ++i)
    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = static_cast<std::uint32_t>(i);
        pub.publish(std::move(loan));
        n.drive();
    }
    return 0;
}

}

TEST_CASE("integration.recording_qos a node declaring no recording QoS ships zero capture",
          "[integration][inproc][recording]")
{
    net n{base_opts(/*eager=*/false)};   // producer leaves node_options.capture at the off default
    n.connect();

    counting_codec codec;
    auto encodes = codec.encodes;
    typed_subscriber sub{n.a, "topic", [](const sample &) {}};
    typed_publisher pub{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    publish_k(pub, n, 5);
    REQUIRE(encodes->load() == 0);   // the off default selects nothing: the gate stays inert
}

TEST_CASE("integration.recording_qos a node-level payload default fires the encode per publish",
          "[integration][inproc][recording]")
{
    plexus::node_options producer = base_opts(/*eager=*/false);
    producer.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    net n{producer};
    n.connect();

    counting_codec codec;
    auto encodes = codec.encodes;
    typed_subscriber sub{n.a, "topic", [](const sample &) {}};
    typed_publisher pub{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    constexpr int k = 5;
    publish_k(pub, n, k);
    REQUIRE(encodes->load() == k);   // the node default selects every topic for payload capture
}

TEST_CASE("integration.recording_qos a per-topic publisher override raises capture above an off node default",
          "[integration][inproc][recording]")
{
    net n{base_opts(/*eager=*/false)};   // node default off: an unoverridden topic stays inert
    n.connect();

    // The selected topic carries a per-topic payload override; the bystander topic relies on
    // the (off) node default. Only the overridden topic encodes.
    counting_codec selected_codec;
    counting_codec bystander_codec;
    auto selected_encodes = selected_codec.encodes;
    auto bystander_encodes = bystander_codec.encodes;

    typed_subscriber sub_sel{n.a, "selected", [](const sample &) {}};
    typed_subscriber sub_by{n.a, "bystander", [](const sample &) {}};

    plexus::typed_publisher_options sel_opts;
    sel_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    typed_publisher pub_sel{n.b, "selected", sel_opts, selected_codec};
    typed_publisher pub_by{n.b, "bystander", plexus::typed_publisher_options{}, bystander_codec};
    n.drive();

    constexpr int k = 4;
    publish_k(pub_sel, n, k);
    publish_k(pub_by, n, k);

    REQUIRE(selected_encodes->load() == k);    // the per-topic override took effect
    REQUIRE(bystander_encodes->load() == 0);   // the off node default left the bystander inert
}
