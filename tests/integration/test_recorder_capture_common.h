#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORDER_CAPTURE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORDER_CAPTURE_COMMON_H

// The recorder oracle, in two halves.
//
// (1) The pre-buffer / FDR mode (the same byte_ring run drop-oldest continuous-overwrite):
// it holds "the last N bytes" (byte-bounded under a saturating producer), a freeze
// snapshots two indices with zero allocation (no buffer copy), the frozen window drains to
// the in-memory byte_sink with no thread, all three trigger sources fire (manual, an armed
// anomaly predicate on a drop edge, a deadline-miss edge), and captured_span equals
// newest_ts - oldest_ts under a manual clock.
//
// (2) The public-API end-to-end capture (the make_recorder attach + the RAII handle): a
// live multi-endpoint inproc node attaches a recorder through node.make_recorder(sink,
// opts) — PUBLIC API ONLY, no router(), no internal recording include in the attach path —
// runs a multi-topic session, drains on the node's own executor turns, recovers the flat
// stream offline with record_stream_reader, decodes the payload with a separately-supplied
// codec (no codec in the tap), and closes the dropout accounting
// sum(dropout.count) + delivered == produced. A teardown section destroys the recorder and
// pumps the executor post-dtor — this section exercises the runtime behavior of
// make_recorder + the recorder RAII handle (the deregister-before-teardown discipline); the
// asan tree is the load-bearing UAF gate. The capture/dropout legs loop >=3x (medians). No
// tuned byte-budget constant is asserted as a default.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/recorder.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using plexus::io::message_info;
using plexus::io::qos_edge;
using plexus::io::qos_change_event;
using plexus::io::capture_fidelity;
using plexus::io::detail::drop_event;
using plexus::io::detail::drop_cause;
using plexus::io::locality;
using plexus::io::recording::record_category;
using plexus::io::recording::record_envelope;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;
using plexus::io::recording::pre_buffer_controller;

namespace recorder_capture_fixture {

// A deterministic monotonic clock: each read advances by a fixed step so captured
// timestamps are exact and the time-span is a known multiple of the step.
struct manual_clock
{
    std::uint64_t now{0};
    std::uint64_t step{10};
    std::uint64_t operator()() noexcept
    {
        const std::uint64_t t = now;
        now += step;
        return t;
    }
};

inline std::vector<std::byte> payload_of(std::size_t n, std::byte fill)
{
    return std::vector<std::byte>(n, fill);
}

// Decode the frozen window the FDR drain wrote to the sink. The drained bytes are a bare
// length-prefixed record stream (no stream header — the window is the ring's records), so
// iterate [varint len][payload] and decode each body (CRC stripped) offline.
inline std::vector<decoded_record> decode_window(std::span<const std::byte> stream)
{
    std::vector<decoded_record> out;
    std::size_t                 at = 0;
    while(at < stream.size())
    {
        std::size_t len_off = at;
        const auto  len     = plexus::wire::read_varint(stream, len_off);
        if(!len)
            break;
        const std::size_t end = len_off + static_cast<std::size_t>(*len);
        if(end > stream.size())
            break;
        const auto payload = stream.subspan(len_off, static_cast<std::size_t>(*len));
        if(payload.size() >= sizeof(std::uint32_t))
        {
            decoded_record rec;
            const auto     body = payload.first(payload.size() - sizeof(std::uint32_t));
            if(plexus::io::recording::decode_record_body(body, rec))
                out.push_back(rec);
        }
        at = end;
    }
    return out;
}

inline drop_event arq_drop()
{
    drop_event e;
    e.cause      = drop_cause::arq_shed;
    e.transport  = locality::any;
    e.topic_hash = 0x1234;
    e.count      = 1;
    return e;
}

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// The fixture OWNS the executor + bus + transport + discovery; the node and the recorder
// are built in an inner scope over this borrowed substrate, so the executor outlives both
// and can be pumped after the recorder's (and the node's) dtor returns — the post-dtor pump
// that proves the deregister-before-teardown discipline.
struct session_fixture
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    static_discovery   disc{{}};

    void drive()
    {
        ex.drain();
    }
};

// The separately-supplied offline decoder, proving no codec lives in the tap: a captured
// sample carries the framed wire bytes, whose tail is the published payload. This recovers
// the marker by matching the trailing bytes — the stream itself never decoded them.
inline bool payload_ends_with(std::span<const std::byte> framed, std::span<const std::byte> marker)
{
    if(framed.size() < marker.size())
        return false;
    return std::equal(marker.begin(), marker.end(), framed.end() - marker.size());
}

}

#endif
