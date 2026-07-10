// The host-side flat-stream to MCAP transcode round-trip
// oracle. A live wire-capturing inproc session is driven through the PUBLIC recording API into an
// in-memory flat stream; the transcode maps it to an MCAP file; the mcap reader reads it back and
// the channel / schema / message mapping is asserted: a per-topic sample channel carries the
// captured samples (payload bytes byte-identical), synthetic plexus-events channels carry the
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
#include "plexus/type_schema.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"
#include "plexus/recorder_options.h"
#include "plexus/wire_capture_qos.h"

#include "plexus/io/capture_policy.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/io/recording_channel.h"
#include "plexus/io/wire_capturing_transport.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/hints/robotics.h"

#include "plexus/recording/host/file_sink.h"
#include "plexus/recording/host/rotating_sink.h"

#include <mcap/reader.hpp>

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cctype>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <filesystem>
#include <string_view>
#include <unordered_map>
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
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x9A9A0001u, "reading"};
    }
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
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

// Drive a wire-capturing producer + a plain consumer over inproc, publish a run of typed
// readings, drain the recorder, and return the accumulated flat capture bytes.
std::vector<std::byte> capture_session(int count)
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_inner{ex, bus};
    wire_transport producer_tp{producer_inner};

    plexus::node_options consumer_opts = base_opts();
    plexus::node_options producer_opts = base_opts();
    producer_opts.wire                 = plexus::wire_capture_qos{.enabled = true, .position = plexus::wire_crypto_position::cleartext};

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
    std::size_t total_messages{0};
    std::size_t telemetry_messages{0};
    std::size_t wire_messages{0};
    std::size_t wire_meta_messages{0};
    // The published reading values recovered from the raw wire-frame payload tails: the
    // codec writes the value as 4 little-endian bytes which framing leaves at the frame
    // tail, so recovering them proves the raw bytes rode through the transcode byte-
    // identical (the transcode never decodes — it lays the framed bytes in verbatim).
    std::unordered_set<std::uint32_t> wire_tail_values;
    // topic -> (channel messageEncoding, referenced schema encoding or "" for schemaId 0).
    // The mcap-doctor rule Foxglove enforces: a "json" message channel may only reference a
    // schema encoded "jsonschema" (or none) — never one whose own encoding is "json".
    std::unordered_map<std::string, std::pair<std::string, std::string>> channel_encodings;
    // topic -> referenced schema name ("" for schemaId 0), so a well-known-schema decoration
    // can be asserted by the exact schema a channel points at.
    std::unordered_map<std::string, std::string> channel_schema_name;
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
        if(rb.channel_encodings.find(topic) == rb.channel_encodings.end())
        {
            rb.channel_encodings.emplace(topic, std::pair{view.channel->messageEncoding, view.schema ? view.schema->encoding : std::string{}});
            rb.channel_schema_name.emplace(topic, view.schema ? view.schema->name : std::string{});
        }
        if(topic == "plexus/wire/meta")
            ++rb.wire_meta_messages;
        else if(topic == "plexus/wire")
        {
            ++rb.wire_messages;
            if(view.message.dataSize >= 4)
            {
                const std::byte *d = view.message.data + (view.message.dataSize - 4);
                std::uint32_t v    = 0;
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

// The Summary section the transcode must emit so Foxglove does not warn "this file is
// unindexed". NoFallbackScan parses ONLY the Summary section (no sequential rescan), so it
// fails closed when the writer wrote an unindexed file — the regression guard for a relapse
// to noChunking. A real Summary carries chunk indexes and statistics, asserted non-empty.
struct summary_check
{
    bool summary_ok{false};
    std::size_t chunk_indexes{0};
    std::uint64_t message_count{0};
};

summary_check read_summary(const std::filesystem::path &path)
{
    summary_check sc;
    mcap::McapReader reader;
    REQUIRE(reader.open(path.string()).ok());

    const auto status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
    sc.summary_ok     = status.ok();
    sc.chunk_indexes  = reader.chunkIndexes().size();
    if(const auto &stats = reader.statistics())
        sc.message_count = stats->messageCount;
    reader.close();
    return sc;
}

bool has_prefix(const std::unordered_set<std::string> &topics, std::string_view prefix)
{
    for(const auto &t : topics)
        if(t.rfind(prefix, 0) == 0)
            return true;
    return false;
}

std::vector<std::byte> slurp(const std::filesystem::path &path)
{
    std::ifstream in{path, std::ios::binary};
    const std::vector<char> raw{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    std::vector<std::byte> out(raw.size());
    std::transform(raw.begin(), raw.end(), out.begin(), [](char c) { return static_cast<std::byte>(static_cast<unsigned char>(c)); });
    return out;
}

}

TEST_CASE("mcap transcode round-trips a captured session through the mcap reader", "[mcap_transcode][mcap]")
{
    const int count = 8;
    const auto flat = capture_session(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_roundtrip.mcap"};
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

    // Every "json" message channel references a legal schema encoding ("jsonschema" or
    // none) — the exact rule Foxglove rejected when the transcode emitted "json" as the
    // schema encoding. The synthesized event + wire-meta channels carry a real jsonschema.
    bool saw_jsonschema = false;
    for(const auto &[topic, enc] : rb.channel_encodings)
    {
        const auto &[msg_enc, schema_enc] = enc;
        if(msg_enc == "json")
        {
            REQUIRE((schema_enc.empty() || schema_enc == "jsonschema"));
            if(schema_enc == "jsonschema")
                saw_jsonschema = true;
        }
    }
    REQUIRE(saw_jsonschema);

    // The output is INDEXED: the Summary section parses on its own (no fallback rescan), and
    // carries chunk indexes + statistics. A relapse to an unindexed (noChunking) write — the
    // state Foxglove warns about — fails this guard.
    const auto sc = read_summary(out);
    REQUIRE(sc.summary_ok);
    REQUIRE(sc.chunk_indexes > 0);
    REQUIRE(sc.message_count > 0);

    std::filesystem::remove(out);
}

namespace {

// A codec whose recorded bytes are bare JSON ({"value":N}) so a declared json/jsonschema
// preamble entry is truthful: the payload-fidelity tier stores the codec output verbatim,
// and the transcode labels the channel from the preamble (it validates nothing — the
// honesty is the producer's contract).
struct json_reading
{
    std::uint32_t value{};
};

struct json_reading_codec
{
    using value_type = json_reading;

    plexus::wire_bytes<> encode(const json_reading &v) const
    {
        auto owner                      = std::make_shared<std::string>("{\"value\":" + std::to_string(v.value) + "}");
        std::span<const std::byte> view = std::as_bytes(std::span{owner->data(), owner->size()});
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, json_reading &) const
    {
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x70110001u, "json_reading"};
    }
};

static_assert(plexus::typed_codec<json_reading_codec>);

constexpr std::string_view k_reading_jsonschema = R"({"type":"object","title":"json_reading","properties":{"value":{"type":"integer"}}})";

// Drive a single producer that captures a DECLARED json topic and an UNDECLARED topic, both
// at payload fidelity, and return the flat capture. The declared topic's preamble entry
// labels its channel; the undeclared topic resolves no entry and stays opaque.
std::vector<std::byte> capture_declared_and_opaque(int count)
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, base_opts()};

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    using declared_publisher  = plexus::publisher<json_reading_codec>;
    using declared_subscriber = plexus::subscriber<json_reading_codec>;
    using opaque_publisher    = typed_publisher;
    using opaque_subscriber   = typed_subscriber;

    declared_subscriber d_sub{consumer, "declared.telemetry", [](const json_reading &) {}};
    opaque_subscriber o_sub{consumer, "opaque.telemetry", [](const reading &) {}};

    plexus::typed_publisher_options decl_opts;
    decl_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    plexus::typed_publisher_options opaque_opts;
    opaque_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};

    declared_publisher d_pub{producer, "declared.telemetry", decl_opts, json_reading_codec{}};
    opaque_publisher o_pub{producer, "opaque.telemetry", opaque_opts, reading_codec{}};
    ex.drain();

    for(int i = 0; i < count; ++i)
    {
        auto d_loan = d_pub.borrow();
        REQUIRE(d_loan);
        d_loan->value = static_cast<std::uint32_t>(i);
        d_pub.publish(std::move(d_loan));

        auto o_loan = o_pub.borrow();
        REQUIRE(o_loan);
        o_loan->value = static_cast<std::uint32_t>(i);
        o_pub.publish(std::move(o_loan));
        ex.drain();
    }
    while(recorder.pump())
        ;
    recorder.flush();

    const auto span = sink.bytes();
    return std::vector<std::byte>(span.begin(), span.end());
}

}

TEST_CASE("mcap transcode labels a declared data channel from a codec-keyed provider; undeclared stays opaque", "[mcap_transcode][mcap]")
{
    const int count = 4;
    const auto flat = capture_declared_and_opaque(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_declared.mcap"};
    std::filesystem::remove(out);

    // Bind the declared type's schema through the escape-hatch provider keyed on the codec's own
    // type_info().type_id — the id is stated once in type_info() and read here, never restated.
    std::unordered_map<std::uint64_t, plexus::tools::mcap_schema> declared_schemas;
    declared_schemas.emplace(json_reading_codec{}.type_info().type_id, plexus::tools::mcap_schema{"json", "json_reading", "jsonschema", std::string{k_reading_jsonschema}});
    const auto result = plexus::tools::flat_to_mcap(flat, out, plexus::tools::provider_from_map(std::move(declared_schemas)));
    REQUIRE(result.ok);

    const auto rb = read_mcap(out);
    REQUIRE(rb.topics.count("declared.telemetry") == 1);
    REQUIRE(rb.topics.count("opaque.telemetry") == 1);

    // The declared topic's channel carries the provider's message_encoding + jsonschema.
    const auto declared = rb.channel_encodings.at("declared.telemetry");
    REQUIRE(declared.first == "json");
    REQUIRE(declared.second == "jsonschema");
    REQUIRE(rb.channel_schema_name.at("declared.telemetry") == "json_reading");

    // The undeclared topic resolves no preamble entry and stays opaque (schemaId 0).
    const auto opaque = rb.channel_encodings.at("opaque.telemetry");
    REQUIRE(opaque.first == "plexus/opaque");
    REQUIRE(opaque.second.empty());

    // The json-channel legality rule (the Foxglove doctor rule) still holds for every json
    // channel — including the now-declared data channel.
    for(const auto &[topic, enc] : rb.channel_encodings)
    {
        const auto &[msg_enc, schema_enc] = enc;
        if(msg_enc == "json")
            REQUIRE((schema_enc.empty() || schema_enc == "jsonschema"));
    }

    std::filesystem::remove(out);
}

namespace {

namespace robotics = plexus::hints::robotics;

constexpr std::uint64_t k_pose_type_id = 0x503E0001u;
constexpr std::uint64_t k_scan_type_id = 0x5CA10001u;

// A codec that stamps a robotics concept into its schema_hint; the concept rides the endpoint
// record and the backend translator joins it to a well-known schema at transcode. The encoded
// bytes are irrelevant to the schema decoration (the transcode decodes nothing), so a minimal
// placeholder object is emitted.
template<std::uint64_t TypeId, robotics::kind Concept>
struct hinted_codec
{
    struct value_type
    {
        std::uint32_t value{};
    };

    plexus::wire_bytes<> encode(const value_type &) const
    {
        auto owner                      = std::make_shared<std::string>("{}");
        std::span<const std::byte> view = std::as_bytes(std::span{owner->data(), owner->size()});
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, value_type &) const
    {
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {TypeId, "hinted", robotics::to_hint(Concept)};
    }
};

using pose_codec = hinted_codec<k_pose_type_id, robotics::kind::pose>;
using scan_codec = hinted_codec<k_scan_type_id, robotics::kind::laser_scan>;

static_assert(plexus::typed_codec<pose_codec>);
static_assert(plexus::typed_codec<scan_codec>);

plexus::typed_publisher_options payload_capture_opts()
{
    plexus::typed_publisher_options popts;
    popts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    return popts;
}

// Drive a producer that publishes a pose-hinted topic (a concept the backend maps), a
// laser_scan-hinted topic (a concept the backend does NOT map), and a hint-less topic, all at
// payload fidelity, draining through the given sink. No preamble schema is declared — the pose
// channel's decoration comes solely from the hint carried on the endpoint record.
void run_pose_session(plexus::io::recording::byte_sink &sink, int count)
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, base_opts()};

    auto recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    plexus::subscriber<pose_codec> pose_sub{consumer, "robotics.pose", [](const pose_codec::value_type &) {}};
    plexus::subscriber<scan_codec> scan_sub{consumer, "robotics.scan", [](const scan_codec::value_type &) {}};
    plexus::subscriber<reading_codec> plain_sub{consumer, "robotics.plain", [](const reading &) {}};

    const auto popts = payload_capture_opts();
    plexus::publisher<pose_codec> pose_pub{producer, "robotics.pose", popts, pose_codec{}};
    plexus::publisher<scan_codec> scan_pub{producer, "robotics.scan", popts, scan_codec{}};
    plexus::publisher<reading_codec> plain_pub{producer, "robotics.plain", popts, reading_codec{}};
    ex.drain();

    for(int i = 0; i < count; ++i)
    {
        auto p = pose_pub.borrow();
        REQUIRE(p);
        pose_pub.publish(std::move(p));
        auto s = scan_pub.borrow();
        REQUIRE(s);
        scan_pub.publish(std::move(s));
        auto pl = plain_pub.borrow();
        REQUIRE(pl);
        plain_pub.publish(std::move(pl));
        ex.drain();
    }
    while(recorder.pump())
        ;
    recorder.flush();
}

std::vector<std::byte> capture_hinted(int count)
{
    in_memory_byte_sink sink;
    run_pose_session(sink, count);
    const auto span = sink.bytes();
    return std::vector<std::byte>(span.begin(), span.end());
}

// Drain a pose capture through the bundled rotating_sink across two segments (a caller-forced
// rotation between the batches). Each new segment is primed with the recorder's head+defs
// preamble, so it decodes standalone. Returns the segment file paths in order.
std::vector<std::filesystem::path> capture_pose_rotating(const std::filesystem::path &dir, int count)
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, base_opts()};

    plexus::recording::host::rotating_sink::options ropts;
    ropts.directory = dir;
    ropts.basename  = "pose";
    plexus::recording::host::rotating_sink rot{ropts};

    auto recorder = producer.make_recorder(rot);
    rot.prime_preamble(recorder.preamble());

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    plexus::subscriber<pose_codec> pose_sub{consumer, "robotics.pose", [](const pose_codec::value_type &) {}};
    plexus::publisher<pose_codec> pose_pub{producer, "robotics.pose", payload_capture_opts(), pose_codec{}};
    ex.drain();

    for(int seg = 0; seg < 2; ++seg)
    {
        for(int i = 0; i < count; ++i)
        {
            auto p = pose_pub.borrow();
            REQUIRE(p);
            pose_pub.publish(std::move(p));
            ex.drain();
        }
        while(recorder.pump())
            ;
        recorder.flush();
        if(seg == 0)
            rot.rotate();
    }
    rot.flush();

    const auto segs = rot.segments();
    return std::vector<std::filesystem::path>(segs.begin(), segs.end());
}

}

TEST_CASE("mcap transcode joins a robotics hint to a well-known jsonschema channel; unknown/0 stays opaque", "[mcap_transcode][mcap]")
{
    const int count = 4;
    const auto flat = capture_hinted(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_hinted.mcap"};
    std::filesystem::remove(out);

    // The backend hint-translator is the only vocabulary-aware seam; the consumer provider is
    // empty here so the hint path is exercised on its own.
    const auto result = plexus::tools::flat_to_mcap(flat, out, plexus::tools::schema_provider{}, plexus::tools::well_known_schema_translator());
    REQUIRE(result.ok);

    const auto rb = read_mcap(out);
    REQUIRE(rb.topics.count("robotics.pose") == 1);
    REQUIRE(rb.topics.count("robotics.scan") == 1);
    REQUIRE(rb.topics.count("robotics.plain") == 1);

    // pose is the chosen well-known concept (documented jsonschema-plottable); its channel is a
    // json message channel referencing the well-known jsonschema. The actual GUI plot is the
    // manual verification step and is not asserted here.
    const auto pose = rb.channel_encodings.at("robotics.pose");
    REQUIRE(pose.first == "json");
    REQUIRE(pose.second == "jsonschema");
    REQUIRE(rb.channel_schema_name.at("robotics.pose") == "foxglove.Pose");

    // laser_scan is a real concept the backend does not map -> nullopt -> opaque schemaId 0.
    const auto scan = rb.channel_encodings.at("robotics.scan");
    REQUIRE(scan.first == "plexus/opaque");
    REQUIRE(scan.second.empty());

    // A hint-less codec (schema_hint 0) also falls back to opaque.
    const auto plain = rb.channel_encodings.at("robotics.plain");
    REQUIRE(plain.first == "plexus/opaque");
    REQUIRE(plain.second.empty());

    std::filesystem::remove(out);
}

TEST_CASE("mcap transcode: the consumer type_id provider wins over the hint translator", "[mcap_transcode][mcap]")
{
    const int count = 2;
    const auto flat = capture_hinted(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_override.mcap"};
    std::filesystem::remove(out);

    // The escape hatch: a consumer provider maps the pose type_id to a different schema. It must
    // win over the backend hint-translator (resolution order: provider > hint > preamble > opaque).
    std::unordered_map<std::uint64_t, plexus::tools::mcap_schema> overrides;
    overrides.emplace(k_pose_type_id, plexus::tools::mcap_schema{"json", "consumer.Override", "jsonschema", R"({"type":"object","title":"consumer.Override"})"});

    const auto result = plexus::tools::flat_to_mcap(flat, out, plexus::tools::provider_from_map(std::move(overrides)), plexus::tools::well_known_schema_translator());
    REQUIRE(result.ok);

    const auto rb = read_mcap(out);
    REQUIRE(rb.channel_encodings.at("robotics.pose").second == "jsonschema");
    REQUIRE(rb.channel_schema_name.at("robotics.pose") == "consumer.Override");

    std::filesystem::remove(out);
}

// The DECODE-level acceptance floor: a pose-hinted capture drained through the BUNDLED host
// file_sink and, separately, a two-segment rotating_sink both transcode to an MCAP whose Summary
// decodes standalone (NoFallbackScan: indexed Summary + chunk index + statistics), every json
// channel references only a jsonschema (the Foxglove doctor rule), and the file_sink capture
// joins the well-known foxglove.Pose schema from the codec-carried hint alone. This proves the
// capture DECODES; it CANNOT and does NOT establish "plots in Foxglove" — that is the manual
// human-gated GUI spot-check (examples/recording/foxglove-plot-check.md) that headless execution
// cannot fake.
TEST_CASE("mcap decode-level acceptance: bundled file_sink + rotation decode standalone (never asserts a plot)", "[mcap_transcode][mcap]")
{
    const int count = 4;

    const auto bundled_dir = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_bundled"};
    std::filesystem::remove_all(bundled_dir);
    std::filesystem::create_directories(bundled_dir);

    // (1) The bundled host file_sink: drain a live capture straight to a file, read it back, and
    // transcode. The pose channel's foxglove.Pose decoration is derived from the codec hint alone.
    const auto plxr = bundled_dir / "pose.plxr";
    {
        plexus::recording::host::file_sink fs{plxr};
        run_pose_session(fs, count);
    }
    const auto flat = slurp(plxr);
    REQUIRE(!flat.empty());

    const auto mcap   = bundled_dir / "pose.mcap";
    const auto result = plexus::tools::flat_to_mcap(flat, mcap, plexus::tools::schema_provider{}, plexus::tools::well_known_schema_translator());
    REQUIRE(result.ok);

    const auto rb = read_mcap(mcap);
    REQUIRE(rb.channel_encodings.at("robotics.pose").first == "json");
    REQUIRE(rb.channel_encodings.at("robotics.pose").second == "jsonschema");
    REQUIRE(rb.channel_schema_name.at("robotics.pose") == "foxglove.Pose");
    for(const auto &[topic, enc] : rb.channel_encodings)
        if(enc.first == "json")
            REQUIRE((enc.second.empty() || enc.second == "jsonschema"));

    const auto sc = read_summary(mcap);
    REQUIRE(sc.summary_ok);
    REQUIRE(sc.chunk_indexes > 0);
    REQUIRE(sc.message_count > 0);

    // (2) The bundled rotating_sink: each rotated segment must transcode and decode standalone via
    // its own Summary (the primed preamble makes every segment self-contained).
    const auto rot_dir = std::filesystem::temp_directory_path() / std::filesystem::path{"plexus_transcode_rot"};
    std::filesystem::remove_all(rot_dir);
    std::filesystem::create_directories(rot_dir);

    const auto segments = capture_pose_rotating(rot_dir, count);
    REQUIRE(segments.size() == 2);

    for(std::size_t i = 0; i < segments.size(); ++i)
    {
        const auto seg_flat = slurp(segments[i]);
        REQUIRE(!seg_flat.empty());

        const auto seg_mcap   = rot_dir / std::filesystem::path{"seg_" + std::to_string(i) + ".mcap"};
        const auto seg_result = plexus::tools::flat_to_mcap(seg_flat, seg_mcap, plexus::tools::schema_provider{}, plexus::tools::well_known_schema_translator());
        REQUIRE(seg_result.ok);

        const auto seg_sc = read_summary(seg_mcap);
        REQUIRE(seg_sc.summary_ok);
        REQUIRE(seg_sc.chunk_indexes > 0);
        REQUIRE(seg_sc.message_count > 0);

        const auto seg_rb = read_mcap(seg_mcap);
        for(const auto &[topic, enc] : seg_rb.channel_encodings)
            if(enc.first == "json")
                REQUIRE((enc.second.empty() || enc.second == "jsonschema"));
    }

    std::filesystem::remove_all(bundled_dir);
    std::filesystem::remove_all(rot_dir);
}

namespace {

// The generality gate: no vendor identifier may reach the neutral tiers. The scan set mirrors
// the four-tier boundary — the header-only core + api, the neutral hints module, the host
// recording lib, and the two generic transcode headers. The generic transcode IMPL is not
// scanned: it is the TU that legitimately includes <mcap/…>. "pointcloud" (no underscore)
// targets the vendor concatenation, so the neutral concept "point_cloud" does not trip it.
bool file_names_a_vendor(const std::filesystem::path &file)
{
    std::ifstream in{file, std::ios::binary};
    std::string body{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    std::transform(body.begin(), body.end(), body.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    for(std::string_view needle : {"foxglove", "pointcloud", "sceneupdate", "mcap/"})
        if(body.find(needle) != std::string::npos)
            return true;
    return false;
}

void scan_for_vendor(const std::filesystem::path &root, std::vector<std::string> &hits)
{
    if(std::filesystem::is_regular_file(root))
    {
        if(file_names_a_vendor(root))
            hits.push_back(root.string());
        return;
    }
    if(!std::filesystem::is_directory(root))
        return;
    for(const auto &e : std::filesystem::recursive_directory_iterator{root})
    {
        const auto ext = e.path().extension();
        if(e.is_regular_file() && (ext == ".h" || ext == ".hpp" || ext == ".cpp") && file_names_a_vendor(e.path()))
            hits.push_back(e.path().string());
    }
}

}

TEST_CASE("no vendor identifier reaches core, api, hints, recording-host, or the generic transcode headers", "[mcap_transcode]")
{
    const std::filesystem::path src{PLEXUS_SOURCE_DIR};
    const std::filesystem::path roots[] = {
            src / "lib/plexus-core/include",
            src / "lib/plexus-api/include",
            src / "lib/plexus-hints-robotics/include",
            src / "lib/plexus-recording-host/include",
            src / "tools/mcap_transcode/include/plexus/tools/mcap_schema.h",
            src / "tools/mcap_transcode/include/plexus/tools/flat_to_mcap.h",
    };

    std::vector<std::string> hits;
    for(const auto &r : roots)
        scan_for_vendor(r, hits);

    INFO("vendor-named files: " <<
         [&]
         {
             std::string s;
             for(const auto &h : hits)
                 s += h + "\n";
             return s;
         }());
    REQUIRE(hits.empty());
}
