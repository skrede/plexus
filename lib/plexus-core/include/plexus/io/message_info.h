#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_INFO_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_INFO_H

#include "plexus/publisher_gid.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

// Per-message metadata delivered to the opt-in subscriber callback overload. A
// plain stack POD assembled from already-decoded fields at the receiving session —
// it owns no payload (its lifetime is the callback invocation only) and costs no
// hot-path allocation.
//
// source_identity is std::optional<publisher_gid>: a zero gid is a valid-looking
// value, so absence is a DISTINCT third state ("unknown / source identity not
// requested"), never a sentinel zero gid. It is engaged only when the publisher's
// gid flag rode the frame.
//
// source_timestamp and publication_sequence already ride the v0.1.x wire;
// reception_timestamp is receiver-stamped; from_intra_process is true ONLY on a
// genuine same-process delivery (derived from the channel's locality tier).
struct message_info
{
    std::optional<publisher_gid> source_identity{};
    std::uint64_t                publication_sequence{};
    std::uint64_t                source_timestamp{};
    std::uint64_t                reception_timestamp{};
    bool                         from_intra_process{};
};

}

#endif
