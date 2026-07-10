// The codec-derived identity carry oracle: a typed publisher whose codec type_info() returns a
// type_id, a type_name, and an opaque schema_hint declares a topic, and a recorder attached
// WITHOUT restating that id captures the publisher_declared endpoint record. Reading the flat
// stream back proves the endpoint record auto-populates type_id, type_name, and schema_hint from
// the codec with zero consumer restatement — codec/schema drift is structurally impossible — and
// that an unset schema_hint round-trips as the exact integer 0.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/wire_bytes.h"
#include "plexus/recorder.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"
#include "plexus/recorder_options.h"

#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <chrono>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;

template<std::uint64_t Id, std::uint64_t Hint>
struct hinted_codec
{
    using value_type = std::uint32_t;

    plexus::wire_bytes<> encode(const std::uint32_t &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, std::uint32_t &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {Id, "hinted_reading", Hint};
    }
};

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options base_opts()
{
    plexus::node_options opts;
    opts.dial_eagerly = true;
    return opts;
}

// Capture the publisher_declared endpoint record for "telemetry" produced by a codec carrying the
// given id + hint, with a recorder that restates NOTHING.
template<typename Codec>
plexus::io::recording::decoded_record capture_endpoint()
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};
    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};
    bare_node consumer{ex, disc, make_id(0x2A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x2B), producer_tp, base_opts()};
    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink);

    plexus::subscriber<Codec> sub{consumer, "telemetry", [](const std::uint32_t &) {}};
    plexus::publisher<Codec> pub{producer, "telemetry", {}, Codec{}};
    ex.drain();
    while(recorder.pump())
        ;
    recorder.flush();

    std::vector<std::byte> stream{sink.bytes().begin(), sink.bytes().end()};
    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));
    REQUIRE(defs.schema.empty());

    std::vector<plexus::io::recording::decoded_record> records;
    REQUIRE(reader.recover(records).header_ok);

    const auto telemetry_hash = plexus::wire::fqn_topic_hash("telemetry");
    const auto declared       = static_cast<std::uint8_t>(plexus::io::endpoint_edge::publisher_declared);
    for(const auto &rec : records)
        if(rec.category == plexus::io::recording::record_category::endpoint && rec.edge == declared && rec.topic_hash == telemetry_hash)
            return rec;
    return {};
}

}

TEST_CASE("the endpoint record auto-populates type_id/type_name/schema_hint from the codec", "[type_identity_carry]")
{
    constexpr std::uint64_t k_id   = 0x5E4501u;
    constexpr std::uint64_t k_hint = 0x00A70003u;
    const auto rec                 = capture_endpoint<hinted_codec<k_id, k_hint>>();

    REQUIRE(rec.category == plexus::io::recording::record_category::endpoint);
    REQUIRE(rec.type_id.has_value());
    REQUIRE(*rec.type_id == k_id);
    REQUIRE(rec.type_name == "hinted_reading");
    REQUIRE(rec.schema_hint == k_hint);
}

TEST_CASE("an unset schema_hint round-trips as the exact integer 0", "[type_identity_carry]")
{
    constexpr std::uint64_t k_id = 0x5E4502u;
    const auto rec               = capture_endpoint<hinted_codec<k_id, 0u>>();

    REQUIRE(rec.category == plexus::io::recording::record_category::endpoint);
    REQUIRE(rec.type_id.has_value());
    REQUIRE(*rec.type_id == k_id);
    REQUIRE(rec.type_name == "hinted_reading");
    REQUIRE(rec.schema_hint == 0u);
}
