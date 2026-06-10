#ifndef HPP_GUARD_PLEXUS_IO_FRAGMENTATION_H
#define HPP_GUARD_PLEXUS_IO_FRAGMENTATION_H

#include "plexus/wire/udp_envelope.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::io {

// The shared sizing knobs for the fragment/reassemble path. Sensible defaults, tunable
// up: a later loss/throughput sweep substantiates the production numbers from recorded
// evidence rather than feel.
//
//   max_message_size — the largest logical message a single send may fragment (the
//                      receiver bounds its reassembly against the same ceiling). ~4 MiB
//                      covers a generous large-payload target without inviting an
//                      unbounded amplification surface.
//   min_fragment_payload — the smallest per-fragment DATA slice a sane budget yields. It
//                      pins the worst-case fragment count so the field-width static_assert
//                      can prove the count never overruns the uint16 frag_cnt field.
struct fragmentation_limits
{
    static constexpr std::size_t max_message_size = 4u * 1024u * 1024u;
    static constexpr std::size_t min_fragment_payload = 128u;
};

// The worst-case fragment count: the largest message at the smallest sane fragment. It
// must stay inside the uint16 frag_cnt field — the same field-width discipline the ARQ
// pins its window against the selective-ack bitmap. A budget that drives the fragment
// count past this is a deliberate, visible edit, not a silent wire-field overflow.
constexpr std::size_t max_fragment_count =
        (fragmentation_limits::max_message_size + fragmentation_limits::min_fragment_payload - 1u) /
        fragmentation_limits::min_fragment_payload;

static_assert(max_fragment_count <= 0xFFFFu,
              "max_message_size / min_fragment_payload overruns the uint16 frag_cnt wire "
              "field — shrink max_message_size or raise min_fragment_payload with it");

// The per-fragment framing the datagram AEAD decorator prepends/appends around each
// sealed fragment: an explicit 8-byte sequence + a 1-byte key-epoch on the wire (so the
// receiver reconstructs the nonce on a reordered/dropped datagram) plus the 16-byte
// Poly1305/GCM tag (RFC 8439). This mirrors datagram_authenticated_channel's wire shape
// (seq(8) || epoch(1) || header || ciphertext || tag(16)) WITHOUT a libcrypto include —
// the core bridge stays OpenSSL-free, so the value is duplicated here, not imported. When
// the channel is AEAD-decorated this is subtracted as a SECOND term so a sealed fragment
// still fits the transport budget instead of silently overrunning the MTU.
constexpr std::size_t k_aead_fragment_overhead = 8 + 1 + 16;

// The per-fragment-DATA budget left after the fragment sub-header (and, on an
// AEAD-decorated channel, the tag) is subtracted from the caller's transport budget
// (the channel passes mtu_budget.max_payload; DTLS passes the post-handshake
// DTLS_get_data_mtu — never hardcoded here). A budget at or below the overhead yields
// no room and is clamped to the floor so the splitter always makes forward progress.
constexpr std::size_t effective_fragment_budget(std::size_t transport_budget,
                                                bool aead_decorated = false) noexcept
{
    const std::size_t overhead = wire::udp_fragment_header_overhead +
                                 (aead_decorated ? k_aead_fragment_overhead : 0u);
    if(transport_budget <= overhead + fragmentation_limits::min_fragment_payload)
        return fragmentation_limits::min_fragment_payload;
    return transport_budget - overhead;
}

// The sink the splitter drives once per fragment, in ascending index order: the channel
// encodes each (idx, cnt, slice) via wrap_udp_fragment_into into its OWN reused scratch
// buffer. Fully-qualified move_only_function — io::detail shadows plexus::detail.
using fragment_sink =
        plexus::detail::move_only_function<void(std::uint16_t frag_idx, std::uint16_t frag_cnt,
                                                std::span<const std::byte>)>;

// Split `payload` against a passed-in transport budget into numbered fragments, emitted
// fragment-by-fragment through `sink` — no collected-fragments container is returned and
// no per-message heap is held; the splitter owns nothing. The first frag_cnt-1 fragments
// are each the full
// effective budget; the last carries the remainder. A payload that fits one fragment
// emits exactly one (frag_cnt == 1). Returns the fragment count emitted.
inline std::uint16_t split(std::span<const std::byte> payload, std::size_t transport_budget,
                           std::uint16_t /*msg_id*/, fragment_sink &sink,
                           bool aead_decorated = false)
{
    const std::size_t budget = effective_fragment_budget(transport_budget, aead_decorated);
    const std::size_t total = payload.size();
    const std::size_t count = total == 0 ? 1u : (total + budget - 1u) / budget;
    const auto frag_cnt = static_cast<std::uint16_t>(count);

    for(std::size_t i = 0; i < count; ++i)
    {
        const std::size_t offset = i * budget;
        const std::size_t len = std::min(budget, total - offset);
        if(sink)
            sink(static_cast<std::uint16_t>(i), frag_cnt, payload.subspan(offset, len));
    }
    return frag_cnt;
}

}

#endif
