#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_FORMAT_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_FORMAT_H

#include <cstdint>

namespace plexus::io::recording {

// k_format_version is bumped only on an incompatible layout change. A sync marker is emitted on the
// first record and then every k_sync_interval records.
inline constexpr std::uint32_t k_stream_magic   = 0x504C5852u; // "PLXR"
inline constexpr std::uint16_t k_format_version = 2u;
inline constexpr std::uint32_t k_sync_marker    = 0x9E37C5A1u;
inline constexpr std::uint32_t k_sync_interval  = 64u;

// Where a captured wire frame was tapped relative to the AEAD seal. The on-disk ordinals match the
// public construction-time wire_crypto_position so the recorder forwards without a remap.
enum class capture_crypto_position : std::uint8_t
{
    cleartext  = 0,
    ciphertext = 1,
};

}

#endif
