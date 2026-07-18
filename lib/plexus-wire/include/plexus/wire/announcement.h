#ifndef HPP_GUARD_PLEXUS_WIRE_ANNOUNCEMENT_H
#define HPP_GUARD_PLEXUS_WIRE_ANNOUNCEMENT_H

#include "plexus/wire/reader.h"
#include "plexus/wire/writer.h"
#include "plexus/wire/byte_order.h"
#include "plexus/wire/length_prefixed.h"

#include "plexus/node_id.h"
#include "plexus/discovery/contact_card.h"
#include "plexus/match/key_pattern_bounds.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::wire {

// The discovery announcement: a node's contact card on an open multicast group. It carries
// the same fields as contact_card (node_id + one port per listening transport + schema) plus
// a goodbye flag and a universe id the discovery leaf filters on, and NOTHING else — a node
// advertising topic or key-realm state would leak it on the unauthenticated link. Decoded straight
// off an untrusted datagram, so decode_announcement is hardened (magic prefix + reader latch,
// nullopt on any malformation, never a partial struct).
struct announcement
{
    std::uint8_t version  = static_cast<std::uint8_t>(discovery::k_contact_card_schema_version);
    std::uint8_t flags    = 0;
    std::uint32_t universe = 0;
    plexus::node_id node_id{};
    std::uint64_t ttl_secs = 0;
    std::vector<std::pair<std::string, std::uint16_t>> listens;
    std::string universe_pattern;

    friend bool operator==(const announcement &, const announcement &) = default;
};

// bit0 of flags: the node is leaving the group (a graceful goodbye), so a browser evicts it
// without waiting out the liveliness ttl. Other bits are reserved and held 0.
inline constexpr std::uint8_t k_announcement_goodbye_flag = 0x01;

// bit1 of flags: a universe pattern trails the listens block. The bit is the presence signal —
// encode writes the field only when it is set, decode reads it only when it is set, so a sender
// that never sets it emits a byte layout identical to the pre-pattern one and an older receiver
// that reads the fixed prefix ignores the trailing field. This is the append-only extension point.
inline constexpr std::uint8_t k_announcement_universe_pattern_flag = 0x02;

// Decode-time ceiling on the u16-length-prefixed pattern, held at the same value construction
// caps a key pattern to, so decode never retains bytes the matcher would refuse. A prefix over
// this is refused before any copy off the untrusted datagram.
inline constexpr std::size_t k_universe_pattern_max = match::default_bounds::k_pattern_length;

// "PLX0" — a fixed plexus discovery tag read as a big-endian u32 ('P'<<24 | 'L'<<16 | 'X'<<8
// | '0'). A datagram whose first four bytes do not match is foreign traffic on the shared
// group and is dropped before any later field is trusted. The MCU lwIP variant reuses this.
inline constexpr std::uint32_t k_announcement_magic = 0x504C5830u;

namespace detail {

// fixed prefix byte-sum: magic u32 + version u8 + flags u8 + universe u32 + node_id 16 B
// + ttl varint (worst-case 10) + n_listens u8. The ttl varint is the only variable-width
// field in the prefix; encode_announcement_into sizes it exactly via its written offset.
constexpr std::size_t announcement_fixed_max = 4 + 1 + 1 + 4 + 16 + 10 + 1;

inline std::size_t announcement_encoded_size(const announcement &ann)
{
    std::size_t total = announcement_fixed_max;
    for(const auto &[transport, port] : ann.listens)
        total += 1 + transport.size() + sizeof(std::uint16_t);
    if(ann.flags & k_announcement_universe_pattern_flag)
        total += sizeof(std::uint16_t) + ann.universe_pattern.size();
    return total;
}

inline void decode_listens(reader &r, announcement &ann)
{
    const auto n_listens = r.u8();
    ann.listens.reserve(n_listens);
    for(std::uint8_t i = 0; i < n_listens; ++i)
    {
        const auto transport = r.length_prefixed<std::uint8_t>();
        const auto port      = r.u16();
        if(!r.ok())
            return;
        const auto chars = reinterpret_cast<const char *>(transport.data());
        ann.listens.emplace_back(std::string{chars, transport.size()}, port);
    }
}

}

inline void encode_announcement_into(std::vector<std::byte> &out, const announcement &ann)
{
    out.resize(detail::announcement_encoded_size(ann));
    writer w{out};
    w.u32(k_announcement_magic);
    w.u8(ann.version);
    w.u8(ann.flags);
    w.u32(ann.universe);
    w.bytes(std::span<const std::byte>{ann.node_id.data(), ann.node_id.size()});
    w.varint(ann.ttl_secs);
    w.u8(static_cast<std::uint8_t>(ann.listens.size()));
    for(const auto &[transport, port] : ann.listens)
    {
        w.u8(static_cast<std::uint8_t>(transport.size()));
        w.bytes(std::as_bytes(std::span<const char>{transport.data(), transport.size()}));
        w.u16(port);
    }
    // Append-only: the pattern is the last field, gated on its presence flag, so a flagless emit
    // is byte-identical to the pre-pattern layout and a decoder reading only the prefix ignores it.
    if(ann.flags & k_announcement_universe_pattern_flag)
    {
        w.u16(static_cast<std::uint16_t>(ann.universe_pattern.size()));
        w.bytes(std::as_bytes(std::span<const char>{ann.universe_pattern.data(), ann.universe_pattern.size()}));
    }
    out.resize(w.offset());
}

inline std::vector<std::byte> encode_announcement(const announcement &ann)
{
    std::vector<std::byte> buf;
    encode_announcement_into(buf, ann);
    return buf;
}

// Decode an announcement off an untrusted multicast datagram. The magic and an unknown major
// version short-circuit to nullopt before any later field is trusted; the universe is read and
// retained value-agnostically — decode never rejects on it, the discovery leaf is what filters on
// the value. The whole field list is read against the latching reader, then ok() is tested once: a
// truncated or overlong-prefixed buffer yields nullopt with no partial struct.
inline std::optional<announcement> decode_announcement(std::span<const std::byte> payload)
{
    reader r{payload};
    if(r.u32() != k_announcement_magic)
        return std::nullopt;

    announcement ann;
    ann.version = r.u8();
    if(ann.version != static_cast<std::uint8_t>(discovery::k_contact_card_schema_version))
        return std::nullopt;

    ann.flags    = r.u8();
    ann.universe = r.u32();
    r.copy_to(ann.node_id.data(), ann.node_id.size());
    const auto ttl = r.varint();
    detail::decode_listens(r, ann);

    if(ann.flags & k_announcement_universe_pattern_flag)
    {
        const auto field = r.length_prefixed<std::uint16_t>();
        // Cap before retention: a prefix over the ceiling is refused before any copy off the
        // untrusted datagram. Decode stays value-agnostic — shape validation is the discovery leaf's.
        if(field.size() > k_universe_pattern_max)
            return std::nullopt;
        const auto chars = reinterpret_cast<const char *>(field.data());
        ann.universe_pattern.assign(chars, field.size());
    }

    if(!r.ok() || !ttl)
        return std::nullopt;
    ann.ttl_secs = *ttl;
    return ann;
}

}

#endif
