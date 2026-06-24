// libFuzzer harness over the datagram fragment reassembly surface: the fragment
// sub-header decode plus the bounded reassembler's feed(). Both are fed FRESH per
// call -- a fresh bus/executor/reassembler each input keeps libFuzzer's per-input
// crash isolation clean (a persisted instance would carry partial-message state and
// UB across inputs). The reassembler is the new untrusted-input surface: every field
// is range-checked before it indexes, and feed() must never crash or invoke UB on an
// arbitrary span.

#include "plexus/datagram/detail/reassembler.h"
#include "plexus/io/fragmentation.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_executor.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>

namespace {

using fuzz_executor    = plexus::inproc::inproc_executor<std::chrono::steady_clock>;
using fuzz_timer       = plexus::inproc::inproc_timer<std::chrono::steady_clock>;
using fuzz_reassembler = plexus::datagram::detail::reassembler<fuzz_executor &, fuzz_timer>;

// Read up to two bytes as a big-endian uint16, tolerating a short span (a missing
// byte reads as zero) so the reassembler's own range checks are fuzzed even when the
// sub-header decode fails -- never indexing past the span.
std::uint16_t read_u16_lenient(std::span<const std::byte> b, std::size_t off) noexcept
{
    std::uint16_t hi = off < b.size() ? static_cast<std::uint8_t>(b[off]) : 0u;
    std::uint16_t lo = off + 1 < b.size() ? static_cast<std::uint8_t>(b[off + 1]) : 0u;
    return static_cast<std::uint16_t>((hi << 8) | lo);
}

// Read up to four bytes as a big-endian uint32 with the same short-span tolerance, so the
// widened frag_idx/frag_cnt range (including a near-2^32 forged count) is exercised against
// the reassembler's malformed gate and structural-cost charge.
std::uint32_t read_u32_lenient(std::span<const std::byte> b, std::size_t off) noexcept
{
    std::uint32_t v = 0;
    for(std::size_t i = 0; i < 4; ++i)
        v = (v << 8) | (off + i < b.size() ? static_cast<std::uint8_t>(b[off + i]) : 0u);
    return v;
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus;

    const auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    plexus::inproc::inproc_bus<std::chrono::steady_clock> bus;
    fuzz_executor                                         ex{bus};
    fuzz_reassembler                                      r{ex};

    // (a) Drive the fail-closed fragment sub-header decode over the raw bytes, then
    // feed the decoded fields -- the wire-decode-to-reassembler path on real input.
    if(auto hdr = wire::decode_udp_fragment_header(bytes))
        (void)r.feed(hdr->msg_id, hdr->frag_idx, hdr->frag_cnt, hdr->payload);

    // (b) Independently fuzz the reassembler's own range checks with field values
    // derived directly from the raw bytes (so idx/cnt combinations the decode would
    // never surface are still exercised), feeding the trailing bytes as the payload.
    const auto msg_id   = read_u16_lenient(bytes, 0);
    const auto frag_idx = read_u32_lenient(bytes, 2);
    const auto frag_cnt = read_u32_lenient(bytes, 6);
    const auto payload  = bytes.size() > wire::udp_fragment_subheader ? bytes.subspan(wire::udp_fragment_subheader) : std::span<const std::byte>{};
    (void)r.feed(msg_id, frag_idx, frag_cnt, payload);

    return 0;
}
