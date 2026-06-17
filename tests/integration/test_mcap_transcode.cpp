// The host-side flat-stream to MCAP transcode round-trip oracle. A live wire-capturing
// inproc session is driven through the PUBLIC recording API into an in-memory flat stream;
// the transcode maps it to an MCAP file; the mcap reader reads it back and the channel /
// schema / message mapping is asserted: a per-topic sample channel carries the captured
// samples (payload bytes byte-identical), synthetic plexus-events channels carry the
// control-plane records, and — because this composition opts a transport into wire capture —
// the wire channel carries the framed bytes. The transcode never decodes a payload (the bytes
// round-trip verbatim), proving the serializer-agnostic contract end to end.

#include "in_memory_byte_sink.h"

#include "plexus/tools/flat_to_mcap.h"

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

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <mcap/reader.hpp>

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string_view>
#include <unordered_set>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using plexus::io::wire_capturing_policy;
using plexus::io::wire_capturing_transport;

using wire_policy    = wire_capturing_policy<inproc_policy>;
using wire_transport = wire_capturing_transport<inproc_transport<>, inproc_policy>;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;
using wire_node = plexus::node<wire_policy, wire_transport>;

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

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, reading &out) const
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

plexus::node_options base_opts()
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

// Drive a wire-capturing producer + a plain consumer over inproc, publish a run of typed
// readings, drain the recorder, and return the accumulated flat capture bytes.
std::vector<std::byte> capture_session(int count)
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_inner{ex, bus};
    wire_transport     producer_tp{producer_inner};

    plexus::node_options consumer_opts = base_opts();
    plexus::node_options producer_opts = base_opts();
    producer_opts.wire = plexus::wire_capture_qos{.enabled = true,
                                                  .position = plexus::wire_crypto_position::cleartext};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, consumer_opts};
    wire_node producer{ex, disc, make_id(0x0B), producer_tp, producer_opts};

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};
    typed_publisher pub{producer, "telemetry", plexus::typed_publisher_options{}, reading_codec{}};
    ex.drain();

    for(int i = 0; i < count; ++i)
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

    const auto span = sink.bytes();
    return std::vector<std::byte>(span.begin(), span.end());
}

struct read_back
{
    std::unordered_set<std::string> topics;
    std::size_t                     total_messages{0};
    std::size_t                     telemetry_messages{0};
    std::size_t                     wire_messages{0};
    std::size_t                     wire_meta_messages{0};
    // The published reading values recovered from the raw wire-frame payload tails: the
    // codec writes the value as 4 little-endian bytes which framing leaves at the frame
    // tail, so recovering them proves the raw bytes rode through the transcode byte-
    // identical (the transcode never decodes — it lays the framed bytes in verbatim).
    std::unordered_set<std::uint32_t> wire_tail_values;
};

read_back read_mcap(const std::filesystem::path &path)
{
    read_back rb;
    mcap::McapReader reader;
    const auto status = reader.open(path.string());
    REQUIRE(status.ok());

    for(const auto &view : reader.readMessages())
    {
        ++rb.total_messages;
        const std::string topic = view.channel->topic;
        rb.topics.insert(topic);
        if(topic == "plexus/wire/meta")
            ++rb.wire_meta_messages;
        else if(topic == "plexus/wire")
        {
            ++rb.wire_messages;
            if(view.message.dataSize >= 4)
            {
                const std::byte *d = view.message.data + (view.message.dataSize - 4);
                std::uint32_t    v = 0;
                for(int i = 0; i < 4; ++i)
                    v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(d[i])) << (8 * i);
                rb.wire_tail_values.insert(v);
            }
        }
        else if(topic.rfind("plexus/topic/", 0) == 0 || topic == "telemetry")
            ++rb.telemetry_messages;
    }
    reader.close();
    return rb;
}

bool has_prefix(const std::unordered_set<std::string> &topics, std::string_view prefix)
{
    for(const auto &t : topics)
        if(t.rfind(prefix, 0) == 0)
            return true;
    return false;
}

}

TEST_CASE("mcap transcode round-trips a captured session through the mcap reader",
          "[mcap_transcode][mcap]")
{
    const int  count = 8;
    const auto flat  = capture_session(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() /
                     std::filesystem::path{"plexus_transcode_roundtrip.mcap"};
    std::filesystem::remove(out);

    const auto result = plexus::tools::flat_to_mcap(flat, out);
    REQUIRE(result.ok);
    REQUIRE(result.messages > 0);
    REQUIRE(result.channels > 0);
    REQUIRE(result.recovered >= static_cast<std::size_t>(count));

    const auto rb = read_mcap(out);
    REQUIRE(rb.total_messages > 0);

    // The sample topic round-trips as its own per-topic channel carrying the captured
    // sample records (the wire-capturing producer records its local samples metadata-only,
    // so the channel is present with the per-topic sample count).
    REQUIRE(rb.telemetry_messages >= static_cast<std::size_t>(count));
    REQUIRE(rb.topics.count("telemetry") == 1);

    // Control-plane records ride synthetic plexus-events channels (the session declares
    // endpoints and tears them down, so at least one event channel is present).
    REQUIRE(has_prefix(rb.topics, "plexus/events/"));

    // This composition opts the producer transport into wire capture, so wire_frame records
    // are produced and ride their own channel, with the join keys on the meta channel.
    REQUIRE(rb.wire_messages > 0);
    REQUIRE(rb.wire_meta_messages == rb.wire_messages);
    REQUIRE(rb.topics.count("plexus/wire") == 1);

    // The published reading values ride the framed wire bytes verbatim: every value
    // (0..count-1) is recovered from a wire-frame payload tail — byte-identical round-trip
    // through the transcode with no decode in the path.
    for(std::uint32_t v = 0; v < static_cast<std::uint32_t>(count); ++v)
        REQUIRE(rb.wire_tail_values.count(v) == 1);

    std::filesystem::remove(out);
}
