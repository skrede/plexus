#include "fuzz_sink.h"
#include "fuzz_clock.h"
#include "fuzz_input.h"
#include "fuzz_oracle.h"

#include "plexus/datagram/detail/udp_reliable_arq.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace {

using namespace plexus;

using bytes_view    = std::span<const std::byte>;
using fuzz_bus      = inproc::inproc_bus<fuzz_clock>;
using fuzz_timer    = inproc::inproc_timer<fuzz_clock>;
using fuzz_executor = inproc::inproc_executor<fuzz_clock>;
using fuzz_arq      = datagram::detail::udp_reliable_arq<fuzz_executor &, fuzz_timer>;

constexpr auto k_kind_bits         = std::byte{wire::detail::udp_kind_mask};
constexpr auto k_ignored_flag_bits = ~(k_kind_bits | std::byte{wire::detail::udp_fragmented_bit});

void envelope_leg(bytes_view input)
{
    const auto dec = wire::unwrap_udp(input);
    fuzz_require(dec.has_value() == (input.size() >= wire::udp_envelope_overhead), "an envelope decodes exactly when it holds the fixed overhead");
    if(!dec)
        return;
    fuzz_require(dec->frame.size() + wire::udp_envelope_overhead == input.size() && dec->frame.data() + dec->frame.size() == input.data() + input.size(),
                 "an accepted envelope's frame is the input past the envelope");
    std::vector<std::byte> rewrapped;
    if(dec->fragmented)
        wire::wrap_udp_into_fragmented(rewrapped, dec->kind, dec->seq, dec->frame);
    else
        wire::wrap_udp_into(rewrapped, dec->kind, dec->seq, dec->frame);
    rewrapped[0] |= input[0] & k_ignored_flag_bits;
    fuzz_require(std::ranges::equal(rewrapped, input), "an accepted envelope re-wraps to the input up to the flag bits the decoder ignores");
}

void arq_kind_leg(bytes_view frame)
{
    const auto kind    = wire::peek_udp_arq_kind(frame);
    const auto ack     = wire::decode_udp_ack(frame);
    const auto segment = wire::decode_udp_segment(frame);
    fuzz_require(segment.has_value() == (kind == wire::udp_arq_kind::segment), "peek names a segment exactly when the segment decoder accepts");
    fuzz_require(ack.has_value() == (kind == wire::udp_arq_kind::ack && frame.size() == wire::udp_ack_frame_size),
                 "the ack decoder accepts exactly an ack-marked frame of the fixed ack size");
    std::vector<std::byte> encoded;
    if(ack)
    {
        wire::encode_udp_ack_into(encoded, *ack);
        fuzz_require(std::ranges::equal(encoded, frame), "an accepted ack re-encodes byte-identical");
    }
    if(segment)
    {
        wire::encode_udp_segment_into(encoded, *segment);
        fuzz_require(std::ranges::equal(encoded, frame), "an accepted segment re-encodes byte-identical");
    }
}

void fragment_leg(bytes_view frame)
{
    const auto header = wire::decode_udp_fragment_header(frame);
    fuzz_require(header.has_value() == (frame.size() >= wire::udp_fragment_subheader), "a fragment header decodes exactly when it holds the sub-header");
    if(!header)
        return;
    std::vector<std::byte> encoded;
    wire::encode_udp_fragment_payload_into(encoded, header->msg_id, header->frag_idx, header->frag_cnt, header->payload);
    fuzz_require(std::ranges::equal(encoded, frame), "an accepted fragment header re-encodes to the same header and slice");
}

void handshake_leg(bytes_view input)
{
    const auto hs = datagram::detail::decode_handshake(input);
    fuzz_consume(hs);
    if(!hs)
        return;
    std::vector<std::byte> encoded;
    datagram::detail::encode_handshake_into(encoded, hs->type, hs->mode, hs->initial_seq);
    fuzz_require(std::ranges::equal(encoded, input), "an accepted handshake re-encodes to exactly the input datagram");
    fuzz_require(!wire::peek_udp_arq_kind(input.subspan(wire::udp_envelope_overhead)), "a handshake's inner frame is never an ARQ segment or ack");
}

void submit_step(fuzz_input &in, fuzz_arq &arq, std::size_t window)
{
    const std::size_t before = arq.in_flight();
    const bool admitted      = arq.submit(std::as_bytes(in.take(in.u8()))) == fuzz_arq::submit_result::admitted;
    fuzz_require(admitted == (before < window) && arq.in_flight() == before + (admitted ? 1 : 0), "a submit is admitted exactly while the window has room");
}

void segment_step(fuzz_input &in, fuzz_arq &arq)
{
    const auto seq        = static_cast<std::uint16_t>(in.uint(2));
    const bool fragmented = (in.u8() & 1) != 0;
    if(const auto payload = wire::decode_udp_segment(std::as_bytes(in.take(in.u8()))))
        arq.on_segment(seq, fragmented, *payload);
}

void arq_step(fuzz_input &in, fuzz_arq &arq, fuzz_executor &ex, std::size_t window)
{
    const std::size_t before = arq.in_flight();
    switch(in.u8() % 4)
    {
        case 0:
            submit_step(in, arq, window);
            return;
        case 1:
            if(const auto ack = wire::decode_udp_ack(std::as_bytes(in.take(wire::udp_ack_frame_size))))
                arq.on_ack(*ack);
            fuzz_require(arq.in_flight() <= before, "an ack only ever shrinks the window, never sliding it past the next sequence to send");
            return;
        case 2:
            segment_step(in, arq);
            break;
        default:
            fuzz_clock::advance(std::chrono::milliseconds{static_cast<std::int64_t>(in.uint(2))});
            ex.drain();
            break;
    }
    fuzz_require(arq.in_flight() == before, "receiving and retransmitting leave the send window where it was");
}

void arq_leg(fuzz_input &in)
{
    const std::size_t window = 1 + in.u8();
    auto next_delivery       = static_cast<std::uint16_t>(in.uint(2));
    fuzz_bus bus;
    fuzz_executor ex{bus};
    fuzz_arq arq{ex, datagram::detail::udp_arq_config{.window = window}, next_delivery};
    arq.on_deliver_seq([&next_delivery](std::uint16_t seq, bool, bytes_view) { fuzz_require(seq == next_delivery++, "the receiver delivers in sequence order"); });
    while(!in.exhausted())
        arq_step(in, arq, ex, window);
    arq.cancel();
    ex.drain();
    fuzz_require(arq.pending().count() == 0, "a cancelled ARQ has no timer wait outstanding");
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if(size == 0)
        return 0;
    fuzz_clock::reset();
    const std::span<const std::uint8_t> rest{data + 1, size - 1};
    fuzz_input in{rest};
    switch(data[0] % 5)
    {
        case 0:
            envelope_leg(std::as_bytes(rest));
            break;
        case 1:
            arq_kind_leg(std::as_bytes(rest));
            break;
        case 2:
            fragment_leg(std::as_bytes(rest));
            break;
        case 3:
            handshake_leg(std::as_bytes(rest));
            break;
        default:
            arq_leg(in);
            break;
    }
    return 0;
}
