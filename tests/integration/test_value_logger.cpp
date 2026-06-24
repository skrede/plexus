// over-limit: one cohesive value_logger projection matrix; the CSV/JSON-Lines/text + alloc-gate
// cells share the one decode+projection harness over the typed handle, so splitting them scatters
// that shared fixture The public-API-only proof for the typed value_logger: the codec decodes one
// topic's bytes at the handle, an opt-in projection formats the decoded value into CSV columns /
// JSON-Lines fields, a projection-less streamable type falls back to the operator<< text
// floor, a decode failure is counted and drops its line, and the warmed steady loop
// formats into a reused buffer with zero per-sample heap allocation. PUBLIC includes only
// (no internal recording/store header): the value_logger is the only thing that decodes.
//
// The replaceable global new/delete (support/alloc_counter.h) constrains this to ONE TU
// per executable, so it is its own ctest binary.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"
#include "plexus/value_logger.h"
#include "plexus/value_projection.h"
#include "plexus/value_logger_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <atomic>
#include <sstream>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <streambuf>
#include <string_view>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &r) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(8);
        for(int i = 0; i < 4; ++i)
        {
            (*owner)[i]     = static_cast<std::byte>((r.sensor >> (8 * i)) & 0xff);
            (*owner)[4 + i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        }
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, reading &out) const
    {
        if(b.size() != 8)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        auto u32 = [&](int o)
        {
            std::uint32_t v = 0;
            for(int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(b[o + i])) << (8 * i);
            return v;
        };
        out = {u32(0), u32(4)};
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x5E4501u, "reading"};
    }
};

static_assert(plexus::typed_codec<reading_codec>);

// The opt-in projection for `reading`: two named columns + the field/JSON emit, all
// appending to a caller-owned buffer (no per-record allocation in the emit verbs).
struct reading_projection
{
    std::array<std::string_view, 2> columns() const
    {
        return {"sensor", "value"};
    }

    void emit_fields(const reading &r, std::string &row, char delim) const
    {
        row += std::to_string(r.sensor);
        row += delim;
        row += std::to_string(r.value);
    }

    void emit_json(const reading &r, std::string &obj) const
    {
        obj += "\"sensor\":";
        obj += std::to_string(r.sensor);
        obj += ",\"value\":";
        obj += std::to_string(r.value);
    }
};

static_assert(plexus::value_projection<reading_projection, reading>);

// A projection-less type that is only streamable: it exercises the operator<< text floor.
struct temperature
{
    int celsius{};
};

std::ostream &operator<<(std::ostream &os, const temperature &t)
{
    return os << t.celsius << "C";
}

struct temperature_codec
{
    using value_type = temperature;

    plexus::wire_bytes<> encode(const temperature &t) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((static_cast<std::uint32_t>(t.celsius) >> (8 * i)) & 0xff);
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, temperature &out) const
    {
        if(b.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(b[i])) << (8 * i);
        out.celsius = static_cast<int>(v);
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x7E33u, "temperature"};
    }
};

static_assert(plexus::typed_codec<temperature_codec>);
static_assert(plexus::streamable<temperature>);

plexus::node_options make_opts(std::uint64_t seed, bool eager)
{
    plexus::node_options o;
    o.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    o.redial_seed  = seed;
    o.dial_eagerly = eager;
    return o;
}

plexus::node_id id_of(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

    inproc_node pub_node{ex, disc, id_of(0x0A), ta, make_opts(0xA, /*eager=*/false)};
    inproc_node sub_node{ex, disc, id_of(0x0B), tb, make_opts(0xB, /*eager=*/true)};

    net()
    {
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();
    }

    void drive()
    {
        ex.drain();
    }
};

// A discarding streambuf: every written byte is dropped, so the sink itself never
// allocates. The alloc gate writes the value_logger's records here so only the
// decode+format path is measured, never an ostream's own buffer growth.
struct null_streambuf final : std::streambuf
{
    int_type overflow(int_type c) override
    {
        return c;
    }
    std::streamsize xsputn(const char *, std::streamsize n) override
    {
        return n;
    }
};

std::vector<std::string> lines_of(const std::string &text)
{
    std::vector<std::string> out;
    std::istringstream       is{text};
    std::string              line;
    while(std::getline(is, line))
        out.push_back(line);
    return out;
}

}

TEST_CASE("value logger: a projection emits a named CSV header and per-sample rows", "[integration]")
{
    net                n;
    std::ostringstream out;
    auto logger = n.sub_node.log<reading_codec, reading_projection>("telemetry", plexus::value_logger_options{.out = out, .format = plexus::log_format::csv}, reading_codec{},
                                                                    reading_projection{});
    plexus::publisher<reading_codec> pub{n.pub_node, "telemetry", {}, reading_codec{}};
    n.drive();

    pub.publish(reading{7, 100});
    n.drive();
    pub.publish(reading{7, 200});
    n.drive();

    const auto lines = lines_of(out.str());
    REQUIRE(lines.size() == 3);
    REQUIRE(lines[0] == "publication_sequence,sensor,value");
    REQUIRE(lines[1].find(",7,100") != std::string::npos);
    REQUIRE(lines[2].find(",7,200") != std::string::npos);
    REQUIRE(logger.decode_failed() == 0);
}

TEST_CASE("value logger: a projection emits one named-field JSON object per line", "[integration]")
{
    net                n;
    std::ostringstream out;
    auto logger = n.sub_node.log<reading_codec, reading_projection>("telemetry", plexus::value_logger_options{.out = out, .format = plexus::log_format::jsonl}, reading_codec{},
                                                                    reading_projection{});
    plexus::publisher<reading_codec> pub{n.pub_node, "telemetry", {}, reading_codec{}};
    n.drive();

    pub.publish(reading{3, 42});
    n.drive();

    const auto lines = lines_of(out.str());
    REQUIRE(lines.size() == 1);
    REQUIRE(lines[0] == "{\"sensor\":3,\"value\":42}");
    REQUIRE(logger.decode_failed() == 0);
}

TEST_CASE("value logger: a projection-less streamable type uses the operator<< text floor", "[integration]")
{
    net n;

    SECTION("csv single value column")
    {
        std::ostringstream out;
        auto               logger = n.sub_node.log<temperature_codec>("temp", plexus::value_logger_options{.out = out, .format = plexus::log_format::csv}, temperature_codec{});
        plexus::publisher<temperature_codec> pub{n.pub_node, "temp", {}, temperature_codec{}};
        n.drive();

        pub.publish(temperature{21});
        n.drive();

        const auto lines = lines_of(out.str());
        REQUIRE(lines.size() == 2);
        REQUIRE(lines[0] == "publication_sequence,value");
        REQUIRE(lines[1].find(",21C") != std::string::npos);
    }

    SECTION("json single string field")
    {
        std::ostringstream out;
        auto               logger = n.sub_node.log<temperature_codec>("temp", plexus::value_logger_options{.out = out, .format = plexus::log_format::jsonl}, temperature_codec{});
        plexus::publisher<temperature_codec> pub{n.pub_node, "temp", {}, temperature_codec{}};
        n.drive();

        pub.publish(temperature{21});
        n.drive();

        const auto lines = lines_of(out.str());
        REQUIRE(lines.size() == 1);
        REQUIRE(lines[0] == "{\"value\":\"21C\"}");
    }

    SECTION("text line")
    {
        std::ostringstream out;
        auto               logger = n.sub_node.log<temperature_codec>("temp", plexus::value_logger_options{.out = out, .format = plexus::log_format::text}, temperature_codec{});
        plexus::publisher<temperature_codec> pub{n.pub_node, "temp", {}, temperature_codec{}};
        n.drive();

        pub.publish(temperature{21});
        n.drive();

        const auto lines = lines_of(out.str());
        REQUIRE(lines.size() == 1);
        REQUIRE(lines[0].find("21C") != std::string::npos);
    }
}

TEST_CASE("value logger: a malformed payload is counted and writes no line", "[integration]")
{
    net                n;
    std::ostringstream out;
    auto logger = n.sub_node.log<reading_codec, reading_projection>("telemetry", plexus::value_logger_options{.out = out, .format = plexus::log_format::jsonl}, reading_codec{},
                                                                    reading_projection{});
    // A bytes publisher on the same topic feeds a wrong-length frame the codec rejects.
    plexus::publisher<>              bytes_pub{n.pub_node, "telemetry"};
    plexus::publisher<reading_codec> good_pub{n.pub_node, "telemetry", {}, reading_codec{}};
    n.drive();

    std::array<std::byte, 3> malformed{}; // not 8 bytes -> decode fails
    bytes_pub.publish(std::span<const std::byte>{malformed});
    n.drive();

    REQUIRE(logger.decode_failed() == 1);
    REQUIRE(out.str().empty()); // no partial line for the bad frame

    good_pub.publish(reading{1, 9});
    n.drive();

    REQUIRE(logger.decode_failed() == 1);
    REQUIRE_FALSE(out.str().empty()); // the good frame still projects
}

TEST_CASE("value logger: the warmed steady projection loop is zero per-sample alloc", "[integration]")
{
    constexpr int warm = 64;
    constexpr int K    = 4096;

    net            n;
    null_streambuf sink;
    std::ostream   out{&sink}; // a non-allocating sink: only the format path is measured
    auto           logger = n.sub_node.log<reading_codec, reading_projection>("telemetry", plexus::value_logger_options{.out = out, .format = plexus::log_format::csv}, reading_codec{},
                                                                              reading_projection{});
    plexus::publisher<reading_codec> pub{n.pub_node, "telemetry", {}, reading_codec{}};
    n.drive();

    auto publish = [&](std::uint32_t v)
    {
        pub.publish(reading{7, v});
        n.drive();
    };

    // Warm: grow the bus deque blocks and the logger's reused format buffer to steady
    // state so the measured window touches no first-time growth.
    for(int i = 0; i < warm; ++i)
        publish(static_cast<std::uint32_t>(i));

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        publish(0xF0000000u + static_cast<std::uint32_t>(i));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(logger.decode_failed() == 0);
    REQUIRE(after - before == 0); // the steady decode+format+write path allocated nothing
}
