#include "seed_sink.h"

#include "plexus/datagram/detail/udp_handshake_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/udp_ack.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/udp_envelope.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>
#include <initializer_list>

namespace {

using namespace plexus;
using datagram::detail::udp_hs_type;
using datagram::detail::udp_channel_mode;

constexpr std::uint8_t k_envelope_leg  = 0;
constexpr std::uint8_t k_arq_kind_leg  = 1;
constexpr std::uint8_t k_fragment_leg  = 2;
constexpr std::uint8_t k_handshake_leg = 3;
constexpr std::uint8_t k_arq_leg       = 4;

seed ack_frame(std::uint16_t cumulative, std::initializer_list<std::size_t> holes)
{
    wire::udp_ack ack{.cumulative = cumulative, .selective = {}};
    for(const std::size_t hole : holes)
        ack.mark_hole(hole);
    seed out;
    wire::encode_udp_ack_into(out, ack);
    return out;
}

seed segment_frame(std::string_view payload)
{
    seed out;
    wire::encode_udp_segment_into(out, text(payload));
    return out;
}

seed handshake(udp_hs_type type, udp_channel_mode mode, std::uint16_t initial_seq)
{
    seed out;
    datagram::detail::encode_handshake_into(out, type, mode, initial_seq);
    return out;
}

// Grammar: the leg byte, the window minus one, the initial sequence (little-endian u16), then the steps.
seed arq_open(std::uint8_t window, std::uint16_t initial_seq)
{
    return prefixed({k_arq_leg, static_cast<std::uint8_t>(window - 1), lo(initial_seq), hi(initial_seq)}, {});
}

void arq_submit(seed &s, std::string_view payload)
{
    append(s, {0, lo(payload.size())});
    append(s, text(payload));
}

void arq_ack(seed &s, std::uint16_t cumulative, std::initializer_list<std::size_t> holes)
{
    append(s, {1});
    append(s, ack_frame(cumulative, holes));
}

void arq_segment(seed &s, std::uint16_t seq, std::string_view payload)
{
    const seed frame = segment_frame(payload);
    append(s, {2, lo(seq), hi(seq), 0, lo(frame.size())});
    append(s, frame);
}

// The opcode is shared: 3 advances the ARQ leg's clock, 2 the stream harness's.
void advance(seed &s, std::uint8_t opcode, std::uint16_t ms)
{
    append(s, {opcode, lo(ms), hi(ms)});
}

seed arq_reorder_and_retransmit()
{
    seed s = arq_open(4, 0);
    for(const std::string_view payload : {"a", "b", "c"})
        arq_submit(s, payload);
    arq_ack(s, 0, {0});
    arq_segment(s, 0, "x");
    arq_segment(s, 2, "z");
    arq_segment(s, 1, "y");
    advance(s, 3, 300);
    return s;
}

seed arq_wrap_and_exhaust()
{
    seed s = arq_open(2, 0xfffe);
    for(const std::string_view payload : {"p", "q", "r"})
        arq_submit(s, payload);
    arq_ack(s, 0xfffe, {});
    arq_submit(s, "s");
    for(std::int32_t i = 0; i < 8; ++i)
        advance(s, 3, 2500);
    return s;
}

seed arq_ack_ahead()
{
    seed s = arq_open(8, 100);
    arq_submit(s, "one");
    arq_submit(s, "two");
    arq_ack(s, 1100, {3});
    arq_submit(s, "three");
    return s;
}

// Grammar: the floor in ms minus one, then the throughput floor, the payload cap and the buffer cap above one header,
// each a little-endian u16; then the operations.
seed stream_open(std::uint8_t floor_ms, std::uint16_t throughput, std::uint16_t max_payload, std::uint16_t extra)
{
    return prefixed({static_cast<std::uint8_t>(floor_ms - 1), lo(throughput), hi(throughput), lo(max_payload), hi(max_payload), lo(extra), hi(extra)}, {});
}

void stream_feed(seed &s, std::span<const std::byte> bytes)
{
    while(!bytes.empty())
    {
        const std::size_t n = std::min<std::size_t>(bytes.size(), 255);
        append(s, {0, lo(n)});
        append(s, bytes.first(n));
        bytes = bytes.subspan(n);
    }
}

seed stream_frame(std::size_t payload_size)
{
    const seed payload(payload_size, std::byte{0xab});
    return wire::encode_frame({.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0}, payload);
}

seed stream_with(seed s, std::initializer_list<std::size_t> payload_sizes)
{
    seed bytes;
    for(const std::size_t n : payload_sizes)
        append(bytes, stream_frame(n));
    stream_feed(s, bytes);
    append(s, {1});
    return s;
}

seed stream_split(std::size_t payload_size, std::size_t cut, std::uint16_t pause_ms)
{
    seed s           = stream_open(50, 1024, 1024, 2048);
    const seed frame = stream_frame(payload_size);
    stream_feed(s, std::span{frame}.first(cut));
    advance(s, 2, pause_ms);
    stream_feed(s, std::span{frame}.subspan(cut));
    advance(s, 2, pause_ms);
    return s;
}

}

seed_set udp_envelope_seeds()
{
    seed fragment;
    wire::encode_udp_fragment_payload_into(fragment, 4, 1, 3, text("slice"));
    seed best_effort_fragment;
    wire::wrap_udp_fragment_into(best_effort_fragment, wire::udp_envelope_kind::best_effort, 12, 4, 0, 2, text("first"));
    seed reliable_fragment;
    wire::wrap_udp_into_fragmented(reliable_fragment, wire::udp_envelope_kind::reliable_arq, 5, segment_frame("frag"));
    return {
            prefixed({k_envelope_leg}, wire::wrap_udp(wire::udp_envelope_kind::best_effort, 7, text("frame"))),
            prefixed({k_envelope_leg}, best_effort_fragment),
            prefixed({k_envelope_leg}, reliable_fragment),
            prefixed({k_arq_kind_leg}, ack_frame(9, {0, 3, 255})),
            prefixed({k_arq_kind_leg}, segment_frame("reliable")),
            prefixed({k_fragment_leg}, fragment),
            prefixed({k_handshake_leg}, handshake(udp_hs_type::request, udp_channel_mode::reliable_datagram, 0x1234)),
            prefixed({k_handshake_leg}, handshake(udp_hs_type::response, udp_channel_mode::best_effort, 0)),
            arq_reorder_and_retransmit(),
            arq_wrap_and_exhaust(),
            arq_ack_ahead(),
    };
}

seed_set stream_inbound_seeds()
{
    return {
            stream_with(stream_open(50, 1024, 1024, 2048), {16, 0, 300}), stream_split(40, 10, 5), stream_split(300, 100, 1000), stream_with(stream_open(50, 1024, 16, 2048), {64}),
            stream_with(stream_open(50, 1024, 1024, 0), {100}),
    };
}
