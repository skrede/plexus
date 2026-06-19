#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORDING_CHANNEL_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORDING_CHANNEL_COMMON_H

// The lossless recording_channel decorator proof: send() taps the OUT frame then forwards
// the bytes VERBATIM to the lower channel (forwarded == input), the lower channel's on_data
// taps the IN frame then re-emits the SAME bytes upward, the OUT and IN taps fire once each
// with byte-identical frames, and a wire_record posted through the recording_sink reaches the
// recorder as a wire_frame record whose bytes match the capture. The decorator OWNS its Lower
// (moved in at construction). The trailing static_assert in the header pins byte_channel.

#include "in_memory_byte_sink.h"

#include "plexus/inproc/inproc_policy.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/recording_channel.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <tuple>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>
#include <type_traits>

using plexus::node_id;
using plexus::io::recording_channel;
using plexus::io::recording::flat_recorder;
using plexus::io::recording::recording_sink;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;
using plexus::io::recording::wire_record;
using plexus::io::recording::wire_direction;

namespace recording_channel_fixture {

// A test lower byte_channel: send() records the forwarded bytes and forwards them to an
// injected sink (so a test can drive the peer's lower on_data); feed() drives this channel's
// on_data the way a real reassembled frame would.
class test_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_sent.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_sent});
    }
    void                               close() { m_closed = true; }
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"test", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    std::vector<std::byte>                                               m_sent;
    bool                                                                 m_closed{false};
    std::function<void(std::span<const std::byte>)>                      m_sink;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<test_lower>,
              "test_lower must satisfy byte_channel for the decorator test");
static_assert(plexus::io::byte_channel<recording_channel<test_lower>>,
              "recording_channel<test_lower> must satisfy byte_channel");

// Structural-absence witness (compile-time): a bare channel type is NOT a recording_channel;
// only an explicit specialization is. A default (non-wire) node's byte_channel_type — here the
// inproc policy's — is a bare channel, so the decorator is absent at compile time, not gated by
// a runtime branch. The decorated-vs-bare TYPE is fixed at the mint point.
static_assert(!plexus::io::is_recording_channel_v<test_lower>,
              "a bare channel must not be a recording_channel");
static_assert(!plexus::io::is_recording_channel_v<plexus::io::polymorphic_byte_channel>,
              "the erased channel must not be a recording_channel");
static_assert(
        !plexus::io::is_recording_channel_v<plexus::inproc::inproc_policy::byte_channel_type>,
        "the default inproc channel_type must not be a recording_channel — structurally absent");
static_assert(plexus::io::is_recording_channel_v<recording_channel<test_lower>>,
              "an explicit recording_channel specialization must witness presence");

inline node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0]  = std::byte{tag};
    n[15] = std::byte{0xCD};
    return n;
}

inline std::vector<std::byte> blob(std::uint8_t base, std::size_t n)
{
    std::vector<std::byte> v;
    for(std::size_t i = 0; i < n; ++i)
        v.push_back(static_cast<std::byte>((base + i) & 0xffu));
    return v;
}

}

#endif
