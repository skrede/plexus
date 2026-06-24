#ifndef HPP_GUARD_PLEXUS_IO_FRAGMENTATION_H
#define HPP_GUARD_PLEXUS_IO_FRAGMENTATION_H

#include "plexus/topic_qos.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/detail/compat.h"

#include <span>
#include <limits>
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
//                      can prove the count never overruns the uint32 frag_cnt field.
struct fragmentation_limits
{
    static constexpr std::size_t max_message_size     = 4u * 1024u * 1024u;
    static constexpr std::size_t min_fragment_payload = 128u;
};

// THE NODE-OPTIONS MESSAGE-SIZE DEFAULTS — the operator-facing knobs the transport
// ctors bind by default. The live per-message ceiling is no longer the implicit
// fragmentation_limits::max_message_size (that constant survives only as the
// fragment-count assert math + a safe fallback); it is this settable node default,
// resolved per-topic through effective_max and threaded from the transport ctor.
//
// 8 MiB covers a 1080p raw RGB frame (~5.9 MiB) / RGBA (~7.9 MiB) / depth (~4 MiB)
// plus typical point clouds — double the prior 4 MiB and half the aggregate budget;
// a 4K raw frame (~23.7 MiB) needs an explicit per-topic override or a raised default.
constexpr std::size_t global_default_max_message_bytes = 8u * 1024u * 1024u;

// The always-on aggregate reassembly-memory backstop (the reassembler's
// total_memory_cap): bounds attacker-controlled memory across ALL topics on a
// connection regardless of any single message's declared size, so it is the real DoS
// bound even when the per-topic ceiling is unlimited. Connection-shared, configurable.
constexpr std::size_t reassembly_memory_budget = 16u * 1024u * 1024u;

// The opt-in "unbounded per-message" sentinel for the node default: a topic resolving
// to this has no per-message ceiling, so the aggregate reassembly_memory_budget is the
// only bound. The budget always wins (it gates admit independently of the per-topic max).
constexpr std::size_t k_unlimited_message_bytes = std::numeric_limits<std::size_t>::max();

// The per-message size ceiling for a topic: its declared per-topic override when set,
// else the node default. A pure relation over topic_qos (no I/O) — co-located with the
// size policy, mirroring qos_rxo.h's pure-relation placement.
[[nodiscard]] constexpr std::size_t effective_max(const topic_qos &t, std::size_t global_default) noexcept
{
    return t.max_message_bytes != 0 ? static_cast<std::size_t>(t.max_message_bytes) : global_default;
}

// The worst-case fragment count: the largest message at the smallest sane fragment. It
// must stay inside the uint32 frag_cnt field — the same field-width discipline the ARQ
// pins its window against the selective-ack bitmap. A budget that drives the fragment
// count past this is a deliberate, visible edit, not a silent wire-field overflow.
constexpr std::size_t max_fragment_count = (fragmentation_limits::max_message_size + fragmentation_limits::min_fragment_payload - 1u) / fragmentation_limits::min_fragment_payload;

static_assert(max_fragment_count <= 0xFFFFFFFFu,
              "max_message_size / min_fragment_payload overruns the uint32 frag_cnt wire "
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
constexpr std::size_t effective_fragment_budget(std::size_t transport_budget, bool aead_decorated = false) noexcept
{
    const std::size_t overhead = wire::udp_fragment_header_overhead + (aead_decorated ? k_aead_fragment_overhead : 0u);
    if(transport_budget <= overhead + fragmentation_limits::min_fragment_payload)
        return fragmentation_limits::min_fragment_payload;
    return transport_budget - overhead;
}

// The sink the splitter drives once per fragment, in ascending index order: the channel
// encodes each (idx, cnt, slice) via wrap_udp_fragment_into into its OWN reused scratch
// buffer. Fully-qualified move_only_function — io::detail shadows plexus::detail.
using fragment_sink = plexus::detail::move_only_function<void(std::uint32_t frag_idx, std::uint32_t frag_cnt, std::span<const std::byte>)>;

// Split `payload` against a passed-in transport budget into numbered fragments, emitted
// fragment-by-fragment through `sink` — no collected-fragments container is returned and
// no per-message heap is held; the splitter owns nothing. The first frag_cnt-1 fragments
// are each the full
// effective budget; the last carries the remainder. A payload that fits one fragment
// emits exactly one (frag_cnt == 1). Returns the fragment count emitted.
inline std::uint32_t split(std::span<const std::byte> payload, std::size_t transport_budget, std::uint16_t /*msg_id*/, fragment_sink &sink, bool aead_decorated = false)
{
    const std::size_t budget   = effective_fragment_budget(transport_budget, aead_decorated);
    const std::size_t total    = payload.size();
    const std::size_t count    = total == 0 ? 1u : (total + budget - 1u) / budget;
    const auto        frag_cnt = static_cast<std::uint32_t>(count);

    for(std::size_t i = 0; i < count; ++i)
    {
        const std::size_t offset = i * budget;
        const std::size_t len    = std::min(budget, total - offset);
        if(sink)
            sink(static_cast<std::uint32_t>(i), frag_cnt, payload.subspan(offset, len));
    }
    return frag_cnt;
}

}

#endif
