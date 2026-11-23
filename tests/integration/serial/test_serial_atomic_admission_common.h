#ifndef HPP_GUARD_TESTS_INTEGRATION_SERIAL_ATOMIC_ADMISSION_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SERIAL_ATOMIC_ADMISSION_COMMON_H

#include "plexus/stream/detail/send_queue.h"

#include "plexus/stream/crc_serial.h"

#include "plexus/wire_bytes.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace serial_admission_fixture {

namespace wire   = plexus::wire;
namespace stream = plexus::stream;

using send_queue = stream::detail::send_queue;

// Snapshots the gathered wire bytes per drain turn and parks each completion so the drain is driven
// by hand — the exactly-one-write-outstanding discipline.
struct rec_sink
{
    std::vector<std::vector<std::byte>> turns;
    std::vector<std::size_t> views_per_turn;
    std::vector<send_queue::completion> pending;

    send_queue::send_sink sink()
    {
        return [this](send_queue::buffer_sequence views, send_queue::completion done)
        {
            std::vector<std::byte> flat;
            for(const auto &v : views)
                flat.insert(flat.end(), v.begin(), v.end());
            views_per_turn.push_back(views.size());
            turns.push_back(std::move(flat));
            pending.push_back(std::move(done));
        };
    }

    std::vector<std::byte> wire_stream() const
    {
        std::vector<std::byte> out;
        for(const auto &t : turns)
            out.insert(out.end(), t.begin(), t.end());
        return out;
    }
};

// A stand-in channel: the egress verbs serial_bootstrap can reach, each forwarding to the real
// send_queue and counting the full-queue rejections the production channel surfaces.
struct mock_channel
{
    send_queue queue;
    std::int32_t full_events;

    mock_channel(send_queue::send_sink s, std::size_t cap)
            : queue(std::move(s), cap)
            , full_events(0)
    {
    }

    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!queue.enqueue(bytes))
            ++full_events;
    }
    void enqueue_egress_owned(plexus::wire_bytes<> bytes)
    {
        if(!queue.enqueue(std::move(bytes)))
            ++full_events;
    }
    void enqueue_egress_unit(std::span<plexus::wire_bytes<>> nodes)
    {
        if(!queue.enqueue_unit(nodes))
            ++full_events;
    }
};

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::vector<std::byte> framed_message(const std::string &payload)
{
    const wire::frame_header hdr{.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0};
    return wire::encode_frame(hdr, as_bytes(payload));
}

inline plexus::wire_bytes<> owned_of(const std::vector<std::byte> &bytes, std::shared_ptr<std::vector<std::byte>> &keep)
{
    keep = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
    return plexus::wire_bytes<>{std::span<const std::byte>{*keep}, keep};
}

// The peer's view of the wire: how many well-formed frames verified and how many torn spans dropped.
struct verify_result
{
    std::int32_t matches{0};
    std::int32_t drops{0};
    std::vector<std::byte> last_match;
};

inline verify_result verify_wire(std::span<const std::byte> bytes)
{
    verify_result r;
    stream::crc_serial_inbound dec;
    dec.on_match(
            [&](std::span<const std::byte> f)
            {
                ++r.matches;
                r.last_match.assign(f.begin(), f.end());
            });
    dec.on_drop([&](wire::close_cause) { ++r.drops; });
    dec.feed(bytes);
    return r;
}

}

#endif
