#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_DETAIL_RECORD_SCAN_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_DETAIL_RECORD_SCAN_H

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"

#include "plexus/wire/cursor.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/varint.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

// The offline frame-scan + CRC-verify helpers for record_stream_reader, relocated out of the
// reader header (free functions over the validated wire::reader / the raw stream span).
namespace plexus::io::recording::detail {

inline std::vector<std::byte> read_bytes(wire::reader &r)
{
    const std::uint64_t len = r.varint().value_or(0);
    const auto          b   = r.bytes(static_cast<std::size_t>(len));
    return {b.begin(), b.end()};
}

inline std::string read_string(wire::reader &r)
{
    const std::uint64_t len = r.varint().value_or(0);
    const auto          b   = r.bytes(static_cast<std::size_t>(len));
    return {reinterpret_cast<const char *>(b.data()), b.size()};
}

inline bool is_sync(std::span<const std::byte> payload) noexcept
{
    if(payload.size() != sizeof(std::uint32_t))
        return false;
    wire::reader r{payload};
    return r.u32() == k_sync_marker;
}

// Scan forward from `from` for the next on-disk sync record ([varint len][marker]) and return the
// offset at which it begins; returns the stream end if none remains. The reader then re-reads that
// offset as a sync record and resumes past it.
[[nodiscard]] inline std::size_t resync_from(std::span<const std::byte> stream, std::size_t from)
{
    for(std::size_t at = from; at < stream.size(); ++at)
    {
        std::size_t off = at;
        const auto  len = wire::read_varint(stream, off);
        if(!len || off + static_cast<std::size_t>(*len) > stream.size())
            continue;
        if(is_sync(stream.subspan(off, static_cast<std::size_t>(*len))))
            return at;
    }
    return stream.size();
}

inline bool validate_and_decode(std::span<const std::byte> payload, decoded_record &rec)
{
    if(payload.size() < sizeof(std::uint32_t))
        return false;
    const std::size_t   body_len = payload.size() - sizeof(std::uint32_t);
    wire::reader        cr{payload.subspan(body_len)};
    const std::uint32_t stored = cr.u32();
    if(wire::crc32c(payload.first(body_len)) != stored)
        return false;
    return decode_record_body(payload.first(body_len), rec);
}

}

#endif
