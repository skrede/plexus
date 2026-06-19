#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_DETAIL_RECORD_FRAME_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_DETAIL_RECORD_FRAME_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/crc32c.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

// The envelope-frame + CRC/sync-marker helpers for record_stream_writer, relocated out of the
// writer header (free functions over the caller's scratch buffer).
namespace plexus::io::recording::detail {

inline std::span<const std::byte> as_bytes(std::string_view s) noexcept
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline void blob(wire::writer &w, std::span<const std::byte> b) noexcept
{
    w.varint(b.size());
    w.bytes(b);
}

// Append the CRC-32C over the [category][fields] already written at the buffer front and return the
// contiguous [body][crc] record payload. The caller frames it (the ring varint), so this is the
// recover-validated unit: the scan recomputes the CRC over the payload minus its trailing 4 bytes.
inline std::span<const std::byte> seal_in(std::vector<std::byte> &buf, std::size_t body_len)
{
    const std::uint32_t crc = wire::crc32c({buf.data(), body_len});
    wire::writer        cw{{buf.data() + body_len, sizeof(std::uint32_t)}};
    cw.u32(crc);
    return {buf.data(), body_len + sizeof(std::uint32_t)};
}

}

#endif
