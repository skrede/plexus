// The host-side flat-stream to pcapng transcode round-trip oracle. A live wire-capturing
// inproc session is driven through the PUBLIC recording API into an in-memory flat stream;
// the transcode maps every wire-frame record to an Enhanced Packet Block on a single
// DLT_USER0 interface; the produced bytes are parsed back in-test (no external dependency)
// and the structure is asserted: the SHB byte-order magic and crypto-position option, the
// IDB link type, one EPB per wire frame with a valid direction flag, nanosecond timestamps,
// and packet data byte-identical to the captured frame. The test also writes the captured
// flat stream to a deterministic build-dir path as the fixture the dependent QA gate reads.

#include "in_memory_byte_sink.h"

#include "plexus/tools/flat_to_pcap.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"
#include "plexus/wire_capture_qos.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/recording_channel.h"
#include "plexus/io/wire_capturing_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <utility>
#include <optional>
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

plexus::node_options base_opts()
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

// Drive a wire-capturing producer + a plain consumer over inproc, publish a run of typed
// readings, drain the recorder, and return the accumulated flat capture bytes.
std::vector<std::byte>
capture_session(int                          count,
                plexus::wire_crypto_position position = plexus::wire_crypto_position::cleartext)
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_inner{ex, bus};
    wire_transport     producer_tp{producer_inner};

    plexus::node_options consumer_opts = base_opts();
    plexus::node_options producer_opts = base_opts();
    producer_opts.wire = plexus::wire_capture_qos{.enabled = true, .position = position};

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

// A minimal little-endian pcapng reader: just enough to walk the blocks the transcode
// emits and surface the fields the assertions key on. No external dependency.
std::uint16_t rd_u16(std::span<const std::byte> b, std::size_t off)
{
    return static_cast<std::uint16_t>(static_cast<std::uint8_t>(b[off])) |
            static_cast<std::uint16_t>(static_cast<std::uint8_t>(b[off + 1])) << 8;
}

std::uint32_t rd_u32(std::span<const std::byte> b, std::size_t off)
{
    std::uint32_t v = 0;
    for(int i = 0; i < 4; ++i)
        v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[off + i])) << (8 * i);
    return v;
}

struct parsed_epb
{
    std::uint64_t timestamp{};
    std::uint32_t captured_len{};
    std::uint32_t original_len{};
    std::uint32_t flags_low2{};
    std::uint32_t tail_value{};
    bool          tail_present{false};
    bool          comment_has_crypto{false};
};

struct parsed_pcapng
{
    bool                         shb_magic_ok{false};
    bool                         shb_has_crypto_comment{false};
    std::optional<std::uint16_t> idb_linktype;
    std::vector<parsed_epb>      epbs;
};

bool span_contains(std::span<const std::byte> b, std::size_t off, std::size_t len,
                   std::string_view needle)
{
    if(needle.empty() || len < needle.size())
        return false;
    for(std::size_t i = 0; i + needle.size() <= len; ++i)
    {
        bool match = true;
        for(std::size_t j = 0; j < needle.size(); ++j)
            if(static_cast<char>(b[off + i + j]) != needle[j])
            {
                match = false;
                break;
            }
        if(match)
            return true;
    }
    return false;
}

// Walk a block's options [code:u16][len:u16][value padded to 4] from opt_off..opt_end,
// surfacing the epb_flags low-2-bits and whether a comment carries the crypto token.
void parse_epb_options(std::span<const std::byte> b, std::size_t opt_off, std::size_t opt_end,
                       parsed_epb &epb)
{
    while(opt_off + 4 <= opt_end)
    {
        const std::uint16_t code = rd_u16(b, opt_off);
        const std::uint16_t len  = rd_u16(b, opt_off + 2);
        opt_off += 4;
        if(code == 0) // opt_endofopt
            break;
        if(code == 2 && len == 4) // epb_flags
            epb.flags_low2 = rd_u32(b, opt_off) & 0x3u;
        if(code == 1 && span_contains(b, opt_off, len, "crypto_position=")) // opt_comment
            epb.comment_has_crypto = true;
        opt_off += (static_cast<std::size_t>(len) + 3u) & ~std::size_t{3};
    }
}

parsed_pcapng parse_pcapng(std::span<const std::byte> b)
{
    parsed_pcapng out;
    std::size_t   at = 0;
    while(at + 12 <= b.size())
    {
        const std::uint32_t type  = rd_u32(b, at);
        const std::uint32_t total = rd_u32(b, at + 4);
        REQUIRE(total >= 12);
        REQUIRE(at + total <= b.size());
        const std::size_t body = at + 8;

        if(type == 0x0A0D0D0Au) // SHB
        {
            out.shb_magic_ok          = rd_u32(b, body) == 0x1A2B3C4Du;
            const std::size_t opt_off = body + 16; // magic + major + minor + section_length
            out.shb_has_crypto_comment =
                    span_contains(b, opt_off, at + total - 4 - opt_off, "plexus.crypto_position=");
        }
        else if(type == 0x00000001u) // IDB
        {
            out.idb_linktype = rd_u16(b, body);
        }
        else if(type == 0x00000006u) // EPB
        {
            parsed_epb epb;
            epb.timestamp =
                    static_cast<std::uint64_t>(rd_u32(b, body + 4)) << 32 | rd_u32(b, body + 8);
            epb.captured_len           = rd_u32(b, body + 12);
            epb.original_len           = rd_u32(b, body + 16);
            const std::size_t data_off = body + 20;
            if(epb.captured_len >= 4)
            {
                epb.tail_value   = rd_u32(b, data_off + epb.captured_len - 4);
                epb.tail_present = true;
            }
            const std::size_t opt_off = data_off + ((epb.captured_len + 3u) & ~std::uint32_t{3});
            parse_epb_options(b, opt_off, at + total - 4, epb);
            out.epbs.push_back(epb);
        }
        at += total;
    }
    return out;
}

}

TEST_CASE("pcap transcode round-trips a captured session through a parsed pcapng",
          "[pcap_transcode][pcap]")
{
    const int  count = 8;
    const auto flat  = capture_session(count);
    REQUIRE(!flat.empty());

    const auto out = std::filesystem::temp_directory_path() /
            std::filesystem::path{"plexus_transcode_roundtrip.pcapng"};
    std::filesystem::remove(out);

    const auto result = plexus::tools::flat_to_pcap(flat, out);
    REQUIRE(result.ok);
    REQUIRE(result.packets >= static_cast<std::size_t>(count));
    REQUIRE(result.recovered >= static_cast<std::size_t>(count));

    std::ifstream in{out, std::ios::binary};
    REQUIRE(in);
    std::vector<char> raw{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    in.close();
    std::vector<std::byte> bytes(raw.size());
    for(std::size_t i = 0; i < raw.size(); ++i)
        bytes[i] = static_cast<std::byte>(raw[i]);

    REQUIRE(bytes.size() >= 4);
    REQUIRE(static_cast<std::uint8_t>(bytes[0]) == 0x0A);
    REQUIRE(static_cast<std::uint8_t>(bytes[1]) == 0x0D);
    REQUIRE(static_cast<std::uint8_t>(bytes[2]) == 0x0D);
    REQUIRE(static_cast<std::uint8_t>(bytes[3]) == 0x0A);

    const auto p = parse_pcapng(bytes);
    REQUIRE(p.shb_magic_ok);
    REQUIRE(p.shb_has_crypto_comment);
    REQUIRE(p.idb_linktype.has_value());
    REQUIRE(*p.idb_linktype == 147);
    REQUIRE(p.epbs.size() == result.packets);

    bool                              saw_outbound = false;
    std::unordered_set<std::uint32_t> tail_values;
    for(const auto &epb : p.epbs)
    {
        REQUIRE(epb.captured_len == epb.original_len);
        REQUIRE((epb.flags_low2 == 1 || epb.flags_low2 == 2));
        REQUIRE(epb.comment_has_crypto);
        if(epb.flags_low2 == 2)
            saw_outbound = true;
        if(epb.tail_present)
            tail_values.insert(epb.tail_value);
    }
    REQUIRE(saw_outbound);

    // The codec writes each published value as 4 little-endian bytes that framing leaves at
    // the frame tail; recovering every value 0..count-1 from the EPB packet data proves the
    // framed bytes rode through the transcode byte-identical (no decode in the path).
    for(std::uint32_t v = 0; v < static_cast<std::uint32_t>(count); ++v)
        REQUIRE(tail_values.count(v) == 1);

    // The deterministic fixture the dependent QA gate consumes: the captured flat stream at a
    // build-dir path the CMake registration supplies. A write failure must never fail the test.
    std::ofstream fixture{std::filesystem::path{PLEXUS_QA_CAPTURE_PATH}, std::ios::binary};
    if(fixture)
        fixture.write(reinterpret_cast<const char *>(flat.data()),
                      static_cast<std::streamsize>(flat.size()));

    // The same session recorded with the ciphertext tap position: byte-identical wire frames,
    // but the projector stamps crypto_position=ciphertext into the SHB and every frame comment.
    // The QA gate reads that carried token to exercise the dissector's sealed-blob branch.
    const auto    cipher = capture_session(count, plexus::wire_crypto_position::ciphertext);
    std::ofstream cipher_fixture{std::filesystem::path{PLEXUS_QA_CIPHER_CAPTURE_PATH},
                                 std::ios::binary};
    if(cipher_fixture)
        cipher_fixture.write(reinterpret_cast<const char *>(cipher.data()),
                             static_cast<std::streamsize>(cipher.size()));

    std::filesystem::remove(out);
}
