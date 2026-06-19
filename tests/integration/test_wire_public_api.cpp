// The construction-time per-transport wire-capture public-API oracle: using ONLY the public
// node facade + node_options (no internal recording include in the opt-in path), a node
// composed over the wire-capturing policy + transport mints the recording_channel decorator
// at the single channel-mint point, so a recorder attached through node.make_recorder()
// captures wire_frame records off the live session. A node composed over the bare inproc
// policy mints bare channels — the decorator is STRUCTURALLY ABSENT (a compile-time type
// witness proves it), and a recorder on it records no wire_frame. The opt-in is a designated-
// initializer node_options field (consumer-sovereign, RAII), never a setter.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/wire_capture_qos.h"

#include "plexus/io/recording_channel.h"
#include "plexus/io/wire_capturing_transport.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
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

using plexus::io::recording_channel;
using plexus::io::wire_capturing_policy;
using plexus::io::wire_capturing_transport;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;

// The wire-capturing composition over the inproc substrate: the policy's byte_channel_type IS
// the decorator, so the engine minted over it mints recording_channel channels — the wire
// tier is present BY TYPE, fixed at construction.
using wire_policy    = wire_capturing_policy<inproc_policy>;
using wire_transport = wire_capturing_transport<inproc_transport<>, inproc_policy>;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;
using wire_node = plexus::node<wire_policy, wire_transport>;

// Structural-absence witness (compile-time): a default node's channel type is a bare inproc
// channel, NOT a recording_channel; only the wire-capturing node's channel type is the
// decorator. The decorated-vs-bare TYPE is fixed at the node's construction.
static_assert(!plexus::io::is_recording_channel_v<inproc_policy::byte_channel_type>,
              "a default node's channel must NOT be a recording_channel — structurally absent");
static_assert(
        plexus::io::is_recording_channel_v<wire_policy::byte_channel_type>,
        "the wire-capturing node's channel must be a recording_channel — structurally present");

struct reading
{
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes,
                                                   reading                   &out) const
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

    plexus::type_identity type_info() const { return {0x9A9A0001u, "reading"}; }
};

static_assert(plexus::typed_codec<reading_codec>);

using typed_publisher  = plexus::publisher<reading_codec>;
using typed_subscriber = plexus::subscriber<reading_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options base_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = eager;
    return opts;
}

std::size_t count_wire_frames(std::span<const std::byte> stream)
{
    record_stream_reader r{stream};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    REQUIRE(r.recover(out).header_ok);
    std::size_t n = 0;
    for(const auto &rec : out)
        if(rec.category == record_category::wire_frame)
            ++n;
    return n;
}

}

TEST_CASE("public API: a node that opts a transport into wire capture records wire frames",
          "[wire_public_api][wire]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    // The consumer (plain) and the producer (wire-capturing). The producer's node_options
    // DECLARES the wire opt-in (a designated-initializer aggregate field, consumer-sovereign,
    // no setter) and is composed over the wire-capturing policy + transport, which fixes the
    // decorated channel TYPE at its construction.
    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_inner{ex, bus};
    wire_transport     producer_tp{producer_inner};

    plexus::node_options consumer_opts = base_opts(/*eager=*/true);
    plexus::node_options producer_opts = base_opts(/*eager=*/true);
    producer_opts.wire                 = plexus::wire_capture_qos{
            .enabled = true, .position = plexus::wire_crypto_position::cleartext};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, consumer_opts};
    wire_node producer{ex, disc, make_id(0x0B), producer_tp, producer_opts};

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};
    typed_publisher  pub{producer, "telemetry", plexus::typed_publisher_options{}, reading_codec{}};
    ex.drain();

    for(int i = 0; i < 8; ++i)
    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = static_cast<std::uint32_t>(i);
        pub.publish(std::move(loan));
        ex.drain();
    }
    while(recorder.pump())
        ;
    recorder.flush();

    // The producer's recorder captured framed bytes off the live decorated channel: a node
    // that opted a transport into wire capture produces wire_frame records.
    REQUIRE(count_wire_frames(sink.bytes()) > 0);
}

TEST_CASE("public API: a default node records no wire frames (structural absence)",
          "[wire_public_api][wire]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    // Both nodes default: node_options.wire is left at its disabled default, so each is
    // composed over the bare inproc policy and mints bare channels — the decorator is
    // structurally absent (the compile-time witness above proves the type).
    plexus::node_options consumer_opts = base_opts(/*eager=*/true);
    plexus::node_options producer_opts = base_opts(/*eager=*/true);
    REQUIRE(producer_opts.wire.enabled == false);

    bare_node consumer{ex, disc, make_id(0x1A), consumer_tp, consumer_opts};
    bare_node producer{ex, disc, make_id(0x1B), producer_tp, producer_opts};

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-c:5000"});
    producer.listen({"inproc", "host-d:6000"});
    ex.drain();

    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};
    typed_publisher  pub{producer, "telemetry", plexus::typed_publisher_options{}, reading_codec{}};
    ex.drain();

    for(int i = 0; i < 8; ++i)
    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = static_cast<std::uint32_t>(i);
        pub.publish(std::move(loan));
        ex.drain();
    }
    while(recorder.pump())
        ;
    recorder.flush();

    // No wire opt-in, no decorator composed: the recorder captures the metadata/payload floor
    // but no wire_frame record — the wire tier is structurally absent.
    REQUIRE(count_wire_frames(sink.bytes()) == 0);
}
