#ifndef HPP_GUARD_PLEXUS_WIRE_PEER_REPORT_H
#define HPP_GUARD_PLEXUS_WIRE_PEER_REPORT_H

#include "plexus/wire/topic_declaration.h"

#include "plexus/node_id.h"
#include "plexus/match/key_pattern_bounds.h"

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

// A relay's re-announcement of an attached peer on an authenticated session: the origin identity
// (distinct from the transport-sender), the universe it lives in, a hop count, a per-origin seq, and
// the origin's topics-with-types. It rides a NEW session verb (msg_type::peer_report), not the
// announcement, because the shipped v0.3.0 announcement decoder accepts unknown flag bits and
// ignores trailing bytes — a rider would decode as a direct peer. Decoded straight off an untrusted
// session frame, so decode_peer_report is hardened (latching reader, cap-before-copy, closed-set
// enum checks, nullopt on any malformation, never a partial struct).
struct peer_report
{
    plexus::node_id origin{};
    std::uint32_t origin_universe = 0;
    std::uint8_t hop              = 0;
    std::uint16_t seq             = 0;
    std::uint8_t flags            = 0;
    std::string origin_universe_pattern;
    std::vector<topic_declaration> topics;

    friend bool operator==(const peer_report &, const peer_report &) = default;
};

// bit0: the origin consents to transitive relay of this report. bit1: the report retires the origin
// (a withdrawal, the announcement goodbye precedent) rather than asserting it.
inline constexpr std::uint8_t k_peer_report_consent_flag    = 0x01;
inline constexpr std::uint8_t k_peer_report_withdrawal_flag = 0x02;

// bit2/bit3 are presence signals for the two append-only trailing blocks: encode writes each block
// only when its bit is set and decode reads it only when set, so a sender that never sets one emits
// a byte layout identical to the pre-field one. These are the record's own extension points.
inline constexpr std::uint8_t k_peer_report_universe_pattern_flag = 0x04;
inline constexpr std::uint8_t k_peer_report_topics_flag          = 0x08;

// Decode-time ceiling on the u16-length-prefixed origin universe pattern, held at the same value the
// announcement pattern caps to, so decode never retains bytes the matcher would refuse.
inline constexpr std::size_t k_peer_report_universe_pattern_max = match::default_bounds::k_pattern_length;

namespace detail {

// Wire layout: origin node_id(16) + origin_universe(4) + hop(1) + seq(2) + flags(1)
//   [ + pattern_len(2) + pattern_bytes                         if k_peer_report_universe_pattern_flag ]
//   [ + n_topics(2) + n_topics * topic_declaration entry       if k_peer_report_topics_flag ],
// each topic entry being topic_hash(8)+type_id(8)+type_state(1)+fqn_len(2)+fqn_bytes+name_len(2)+name_bytes.
constexpr std::size_t peer_report_min_size = 24;

// Decode-time ceiling on the topics-list count, refused before any per-entry read off the untrusted
// frame so a claimed-huge u16 count reserves nothing.
constexpr std::size_t k_peer_report_max_topics = 4096;

}

}

#include "plexus/wire/detail/peer_report_codec.h"

#endif
