// The inproc stamp-demand behavioral gate. The receive-side message_info timestamps are
// demand-driven: a 3-arg subscriber that wants them sees the populated stamps exactly as
// before; a 3-arg subscriber that opts out through subscriber_qos sees a documented 0; and
// the per-topic latch that gates the producer's source-stamp clock read collapses to false
// once every attached subscriber consumes no info (the OR-reduce). The two delivery cells
// ride the same two-node single-bus inproc harness the typed fast-path gates use; the
// latch cell asserts the registry OR-reduce directly (a read-only query, never a setter).
//
// Why the latch cell is registry-level: wants_message_info is local-only and never crosses
// the subscribe wire, so a producer's fan-out entry built from a remote subscribe always
// reads the default true. The OR-reduce therefore only collapses where the real subscriber
// arity is locally visible — proven on a registry holding the genuine local entries.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/subscriber_registry.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    plexus::wire_bytes<> encode(const sample &v) const
    {
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

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0x57A11Du;
    opts.dial_eagerly = eager;
    return opts;
}

struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

// The minimal channel the registry's add_subscriber needs: it reads remote_endpoint().scheme
// once to classify the delivery tier. No bus, no I/O — the registry stores a bare pointer.
struct stub_channel
{
    plexus::io::endpoint ep{"inproc", "stub"};
    plexus::io::endpoint remote_endpoint() const { return ep; }
};

}

TEST_CASE("inproc stamp demand: a 3-arg subscriber with default qos sees populated stamps",
          "[integration][inproc][stamp]")
{
    net n;
    n.connect();

    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic",
                       [&](const sample &, const message_info &info) { infos.push_back(info); }};
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, counting_codec{}};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x1234u;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(infos.size() == 1);
    REQUIRE(infos.front().source_timestamp != 0);
    REQUIRE(infos.front().reception_timestamp != 0);
    REQUIRE(infos.front().from_intra_process);
}

TEST_CASE("inproc stamp demand: a 3-arg subscriber that opts out sees a documented 0 stamp",
          "[integration][inproc][stamp]")
{
    net n;
    n.connect();

    plexus::typed_subscriber_options opts;
    opts.qos.wants_message_info = false;   // an informed 3-arg opt-out: deliver no timestamps

    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic", opts,
                       [&](const sample &, const message_info &info) { infos.push_back(info); }};
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, counting_codec{}};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x5678u;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(infos.size() == 1);
    REQUIRE(infos.front().source_timestamp == 0);        // documented "not stamped"
    REQUIRE(infos.front().reception_timestamp == 0);
    REQUIRE(infos.front().from_intra_process);            // arity-independent, always honest
}

TEST_CASE("inproc stamp demand: the per-topic latch OR-reduces over local subscriber demand",
          "[integration][inproc][stamp]")
{
    using registry = plexus::io::subscriber_registry<stub_channel>;
    registry reg;
    const auto hash = plexus::wire::fqn_topic_hash("topic");

    // An unknown topic latches true (the safe always-on default — it stamps).
    REQUIRE(reg.any_subscriber_wants_info(hash));

    // A lone no-info subscriber (the 2-arg arity) collapses the latch to false.
    stub_channel c_no;
    plexus::io::subscriber_qos qos_no;
    qos_no.wants_message_info = false;
    reg.add_subscriber(hash, "topic", c_no, "node-no", qos_no);
    REQUIRE_FALSE(reg.any_subscriber_wants_info(hash));

    // A second, info-wanting subscriber forces stamping for the whole topic (OR-reduce):
    // a suppressing subscriber never starves a demanding one.
    stub_channel c_yes;
    plexus::io::subscriber_qos qos_yes;   // wants_message_info defaults true
    reg.add_subscriber(hash, "topic", c_yes, "node-yes", qos_yes);
    REQUIRE(reg.any_subscriber_wants_info(hash));

    // Retiring the demander recomputes the latch back to the lone no-info state.
    reg.remove_subscriber(hash, c_yes);
    REQUIRE_FALSE(reg.any_subscriber_wants_info(hash));

    // Retiring the last subscriber returns the topic to the empty-set default true.
    reg.remove_subscriber(hash, c_no);
    REQUIRE(reg.any_subscriber_wants_info(hash));
}
