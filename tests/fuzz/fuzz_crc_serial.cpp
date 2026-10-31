#include "fuzz_sink.h"
#include "fuzz_input.h"
#include "fuzz_oracle.h"
#include "fuzz_serial_stream.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <array>
#include <limits>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace {

using namespace fuzz_serial;

constexpr std::size_t k_max_frame = stream::serial_header_size + k_max_payload + stream::crc_trailer_size;
constexpr std::array<std::byte, k_max_frame> k_zero_pad{};

std::vector<bytes> deframe(bytes_view serial, std::span<const std::uint8_t> chunks)
{
    std::vector<bytes> emitted;
    stream::crc_serial_inbound deframer{k_max_payload};
    deframer.on_match(
            [&emitted](bytes_view frame)
            {
                const auto header = wire::decode_header(frame);
                fuzz_require(header && header->payload_len == frame.size() - wire::header_size, "an emitted span is a well-formed wire frame");
                emitted.emplace_back(frame.begin(), frame.end());
            });
    for(std::size_t i = 0; !serial.empty(); ++i)
    {
        const std::size_t n = chunks.empty() ? serial.size() : std::min<std::size_t>(serial.size(), 1 + chunks[i % chunks.size()]);
        deframer.feed(serial.first(n));
        serial = serial.subspan(n);
    }
    deframer.feed(k_zero_pad);
    return emitted;
}

// A forged frame, both CRCs matching over bytes no encoder wrote, may swallow the frames it covers, so the loss bound
// holds only while every emission is an encoded frame.
void require_bounded_loss(const std::vector<bytes> &emitted, const encoded_stream &s)
{
    auto next = s.frames.begin();
    for(const auto &frame : emitted)
    {
        next = std::find(next, s.frames.end(), frame);
        if(next == s.frames.end())
            return;
        ++next;
    }
    const auto touched = std::count_if(s.extents.begin(), s.extents.end(), [](const frame_extent &e) { return e.touched; });
    const auto missing = static_cast<std::ptrdiff_t>(s.frames.size() - emitted.size());
    fuzz_require(missing <= touched, "a mutated stream loses no more encoded frames than its mutations touched");
}

void fuzz_structured(fuzz_input &in)
{
    encoded_stream s{};
    const std::uint32_t frames = 1 + in.u8() % 4;
    for(std::uint32_t i = 0; i < frames; ++i)
        append_frame(in, s);
    const std::uint32_t mutations = in.u8() % 4;
    for(std::uint32_t i = 0; i < mutations; ++i)
        mutate(in, s);
    const auto emitted = deframe(s.serial, in.take(in.u8() % 16));
    if(mutations == 0)
        fuzz_require((emitted == s.frames) != planted_violation, "an unmutated stream deframes to exactly its encoded frames, in order");
    else if(!s.anchor_in_payload)
        require_bounded_loss(emitted, s);
}

void fuzz_raw(fuzz_input &in)
{
    const auto chunks = in.take(in.u8() % 16);
    const auto serial = in.take(std::numeric_limits<std::size_t>::max());
    fuzz_consume(deframe(std::as_bytes(serial), chunks));
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_input in{std::span<const std::uint8_t>{data, size}};
    if((in.u8() & 1) != 0)
        fuzz_raw(in);
    else
        fuzz_structured(in);
    return 0;
}
