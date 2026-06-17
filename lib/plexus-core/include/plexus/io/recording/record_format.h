#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_FORMAT_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_FORMAT_H

#include <cstdint>

namespace plexus::io::recording {

// The shared format constants of the flat append-only record stream. The magic tags
// a stream so a reader rejects unrelated bytes; the version pins the layout (bumped
// only on an incompatible change — the reserved wire_frame slot means the wire tier
// adds no bump). The sync marker is a fixed sentinel emitted between records so a
// recovery scan can resynchronize the record boundary after skipping a corrupt span.
// A marker is emitted on the first record and then every k_sync_interval records.
inline constexpr std::uint32_t k_stream_magic   = 0x504C5852u; // "PLXR"
inline constexpr std::uint16_t k_format_version = 1u;
inline constexpr std::uint32_t k_sync_marker    = 0x9E37C5A1u;
inline constexpr std::uint32_t k_sync_interval  = 64u;

}

#endif
