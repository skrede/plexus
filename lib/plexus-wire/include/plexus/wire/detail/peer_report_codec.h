#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_PEER_REPORT_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_PEER_REPORT_CODEC_H

#include "plexus/wire/reader.h"
#include "plexus/wire/writer.h"
#include "plexus/wire/peer_report.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::wire {

namespace detail {

// The topics count actually put on the wire: clamped to the decoder's ceiling so the u16 count field
// can never wrap (a >65535-edge origin would otherwise encode a frame that decodes valid with almost
// every topic silently lost). encode and encoded_size share it so the buffer and the count agree.
inline std::size_t effective_topic_count(const peer_report &pr) noexcept
{
    return pr.topics.size() < k_peer_report_max_topics ? pr.topics.size() : k_peer_report_max_topics;
}

inline std::size_t peer_report_encoded_size(const peer_report &pr)
{
    std::size_t total = peer_report_min_size;
    if(pr.flags & k_peer_report_universe_pattern_flag)
        total += sizeof(std::uint16_t) + pr.origin_universe_pattern.size();
    if(pr.flags & k_peer_report_topics_flag)
    {
        total += sizeof(std::uint16_t);
        const std::size_t n = effective_topic_count(pr);
        for(std::size_t i = 0; i < n; ++i)
            total += topic_declaration_min_size + pr.topics[i].fqn.size() + pr.topics[i].type_name.size();
    }
    return total;
}

inline void write_peer_report_topic(writer &w, const topic_declaration &td)
{
    w.u64(td.topic_hash);
    w.u64(td.type_id);
    w.u8(static_cast<std::uint8_t>(td.state));
    write_string(w, td.fqn);
    write_string(w, td.type_name);
}

// Read the append-only origin universe pattern when its presence flag is set, capping the length
// before any retention: a prefix over the ceiling is refused before any copy off the untrusted
// frame. Returns false only on an over-cap prefix, which the caller maps to a whole-frame reject.
inline bool decode_peer_report_pattern(reader &r, peer_report &pr)
{
    if(!(pr.flags & k_peer_report_universe_pattern_flag))
        return true;
    const auto field = r.length_prefixed<std::uint16_t>();
    if(field.size() > k_peer_report_universe_pattern_max)
        return false;
    pr.origin_universe_pattern.assign(reinterpret_cast<const char *>(field.data()), field.size());
    return true;
}

// Read one topic entry, capping its two string lids and closed-set-checking its state byte before
// any copy — a malformation returns false with no partial entry retained.
inline bool decode_peer_report_topic_entry(reader &r, topic_declaration &td)
{
    td.topic_hash    = r.u64();
    td.type_id       = r.u64();
    const auto state = r.u8();
    const auto fqn   = r.length_prefixed<std::uint16_t>();
    const auto name  = r.length_prefixed<std::uint16_t>();
    if(!r.ok() || fqn.size() > k_max_fqn || name.size() > k_max_type_name || state > static_cast<std::uint8_t>(type_state::declared))
        return false;
    td.state = static_cast<type_state>(state);
    td.fqn.assign(reinterpret_cast<const char *>(fqn.data()), fqn.size());
    td.type_name.assign(reinterpret_cast<const char *>(name.data()), name.size());
    return true;
}

// Read the append-only topics-with-types list when its presence flag is set. The count is refused
// against both the ceiling AND the bytes actually remaining before any reserve, so a ~30-byte frame
// claiming thousands of entries cannot force a ~400 KB transient allocation it can never fill.
inline bool decode_peer_report_topics(reader &r, peer_report &pr)
{
    if(!(pr.flags & k_peer_report_topics_flag))
        return true;
    const auto n = r.u16();
    if(!r.ok() || n > k_peer_report_max_topics)
        return false;
    if(static_cast<std::size_t>(n) * topic_declaration_min_size > r.remaining())
        return false;
    pr.topics.reserve(n);
    for(std::uint16_t i = 0; i < n; ++i)
    {
        topic_declaration td{};
        if(!decode_peer_report_topic_entry(r, td))
            return false;
        pr.topics.push_back(std::move(td));
    }
    return true;
}

}

inline std::vector<std::byte> encode_peer_report(const peer_report &pr)
{
    std::vector<std::byte> buf(detail::peer_report_encoded_size(pr));
    writer w{buf};
    w.bytes(std::span<const std::byte>{pr.origin.data(), pr.origin.size()});
    w.u32(pr.origin_universe);
    w.u8(pr.hop);
    w.u16(pr.seq);
    w.u8(pr.flags);
    if(pr.flags & k_peer_report_universe_pattern_flag)
        write_string(w, pr.origin_universe_pattern);
    if(pr.flags & k_peer_report_topics_flag)
    {
        const std::size_t n = detail::effective_topic_count(pr);
        w.u16(static_cast<std::uint16_t>(n));
        for(std::size_t i = 0; i < n; ++i)
            detail::write_peer_report_topic(w, pr.topics[i]);
    }
    return buf;
}

// Decode off an untrusted session frame. The whole field list is read against the latching reader
// and ok() is tested once: a truncated frame, a string or list past its lid, or a state byte outside
// the closed three-state set yields nullopt with no partial struct.
inline std::optional<peer_report> decode_peer_report(std::span<const std::byte> payload)
{
    reader r{payload};
    peer_report pr{};
    r.copy_to(pr.origin.data(), pr.origin.size());
    pr.origin_universe = r.u32();
    pr.hop             = r.u8();
    pr.seq             = r.u16();
    pr.flags           = r.u8();

    if(!detail::decode_peer_report_pattern(r, pr) || !detail::decode_peer_report_topics(r, pr))
        return std::nullopt;
    if(!r.ok())
        return std::nullopt;
    return pr;
}

}

#endif
