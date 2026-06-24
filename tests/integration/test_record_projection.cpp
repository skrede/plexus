// The projector-seam read half + its lifetime oracle. read_projection_input is the single
// shared read every host projector consumes: it decodes the preamble + recovers the record
// set + carries the recovery accounting, and — the load-bearing guarantee here — it OWNS its
// payloads. The drop-input-first case frees and scribbles over the source bytes BEFORE reading
// any recovered payload, then asserts the bytes are still byte-identical: a borrowed span into
// the input would be a use-after-free the mcap round-trip cannot catch (mcap_emitter::write
// copies again before write), so only an explicit drop-first read proves the seam self-contained.

#include "in_memory_byte_sink.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_projection.h"
#include "plexus/io/recording/record_stream_writer.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::io::message_info;
using plexus::io::capture_fidelity;
using plexus::io::topic_capture_rule;
using plexus::io::recording::byte_ring;
using plexus::io::recording::record_category;
using plexus::io::recording::read_projection_input;
using plexus::io::recording::record_stream_writer;

namespace {

node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0]  = std::byte{tag};
    n[15] = std::byte{0xAB};
    return n;
}

std::span<const std::byte> bytes_of(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A flat stream carrying one distinctive payload sample, emitted into an OWNED heap buffer
// (so the oracle can free/overwrite it and witness the seam surviving its loss).
std::vector<std::byte> encode_one_sample(const std::string &payload)
{
    record_stream_writer w;
    byte_ring            ring{64u * 1024u};
    in_memory_byte_sink  sink;

    sink.write(w.begin_stream(7u, make_node(1), topic_capture_rule{}, {}));
    ring.try_push(w.sync_marker());

    message_info info{};
    info.publication_sequence = 42;
    info.source_timestamp     = 100;
    ring.try_push(w.sample(11u, plexus::wire::fqn_topic_hash("topic/a"), info, 0xC0DEu, true, capture_fidelity::payload, bytes_of(payload)));

    while(ring.drain(sink, 4096))
        ;
    return {sink.bytes().begin(), sink.bytes().end()};
}

}

TEST_CASE("projection input survives the input stream being freed before its payloads are read", "[record_projection]")
{
    const std::string payload = "the-quick-brown-fox-projection-payload";

    auto input = std::make_unique<std::vector<std::byte>>(encode_one_sample(payload));
    auto proj  = read_projection_input(std::span<const std::byte>{*input});
    REQUIRE(proj.has_value());
    REQUIRE(proj->recovery.header_ok);
    REQUIRE(proj->records.size() == 1);
    REQUIRE(proj->records[0].category == record_category::sample);

    // Drop-input-first: free the source AND scribble a different byte pattern over a fresh
    // allocation of the same size, so a dangling span into the old buffer would read garbage
    // (or trip asan) rather than the original bytes. Only then read the recovered payload.
    const std::size_t n = input->size();
    input.reset();
    std::vector<std::byte> overwrite(n, std::byte{0xEE});
    REQUIRE(overwrite.size() == n);

    const auto       &got = proj->records[0].payload;
    const std::string recovered{reinterpret_cast<const char *>(got.data()), got.size()};
    REQUIRE(recovered == payload);
}

TEST_CASE("read_projection_input returns nullopt for a non-flat buffer", "[record_projection]")
{
    const std::vector<std::byte> garbage(64, std::byte{0x5A});
    REQUIRE_FALSE(read_projection_input(std::span<const std::byte>{garbage}).has_value());
}
