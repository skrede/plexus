#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORD_STREAM_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORD_STREAM_COMMON_H

// The flat record-stream format oracle. It proves the canonical encoding round-trips
// through the reader (mixed categories, a payload sample byte-identical with no codec in
// the path), that the envelope union RESERVES the wire-record variant now (forward-stable
// format), that a mid-write truncation recovers every complete record and loses only the
// trailing partial (crash-recovery by construction), and that the recording_sink ->
// flat_recorder -> byte_ring -> byte_sink chain captures synthetic observer edges that
// decode offline. The in-memory byte_sink is the non-disk drain target; a codec is
// supplied only in the test to interpret a sample's payload, proving the stream itself
// carries raw bytes.

#include "in_memory_byte_sink.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"
#include "plexus/io/recording/record_stream_writer.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <type_traits>

using plexus::node_id;
using plexus::io::message_info;
using plexus::io::endpoint_edge;
using plexus::io::endpoint_event;
using plexus::io::capture_fidelity;
using plexus::io::topic_capture_rule;
using plexus::io::detail::drop_event;
using plexus::io::detail::drop_cause;
using plexus::io::recording::byte_ring;
using plexus::io::recording::wire_record;
using plexus::io::recording::wire_direction;
using plexus::io::recording::flat_recorder;
using plexus::io::recording::recording_sink;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::schema_definition;
using plexus::io::recording::type_schema_entry;
using plexus::io::recording::capture_crypto_position;
using plexus::io::recording::record_stream_reader;
using plexus::io::recording::record_stream_writer;

namespace record_stream_fixture {

inline node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0]  = std::byte{tag};
    n[15] = std::byte{0xAB};
    return n;
}

inline std::span<const std::byte> bytes_of(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Encode a complete in-memory stream of mixed-category records through a real byte_ring
// drain into an in-memory sink. Returns the framed byte stream the reader consumes.
inline std::vector<std::byte> encode_mixed_stream(record_stream_writer &w, byte_ring &ring,
                                                  in_memory_byte_sink &sink,
                                                  const std::string   &payload)
{
    sink.write(w.begin_stream(7u, make_node(1), topic_capture_rule{}, {}));

    ring.try_push(w.sync_marker());

    message_info info{};
    info.publication_sequence = 42;
    info.source_timestamp     = 100;
    info.reception_timestamp  = 101;
    ring.try_push(w.sample(11u, plexus::wire::fqn_topic_hash("topic/a"), info, 0xC0DEu, true,
                           capture_fidelity::payload, bytes_of(payload)));

    message_info meta{};
    meta.publication_sequence = 7;
    ring.try_push(w.sample(12u, plexus::wire::fqn_topic_hash("topic/b"), meta, 0u, false,
                           capture_fidelity::metadata, {}));

    drop_event de{};
    de.cause      = drop_cause::drop_newest;
    de.topic_hash = plexus::wire::fqn_topic_hash("topic/a");
    de.count      = 5;
    ring.try_push(w.drop(13u, de));

    endpoint_event ee{};
    ee.edge       = endpoint_edge::publisher_declared;
    ee.topic_hash = plexus::wire::fqn_topic_hash("topic/a");
    ee.type_id    = 0xC0DEu;
    ring.try_push(w.endpoint(14u, "topic/a", ee));

    while(ring.drain(sink, 4096))
        ;

    return {sink.bytes().begin(), sink.bytes().end()};
}

}

#endif
