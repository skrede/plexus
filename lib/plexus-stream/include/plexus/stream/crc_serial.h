#ifndef HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H
#define HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H

#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/close_cause.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::stream {

inline constexpr std::size_t crc_trailer_size = 4;

// The CRC32C integrity trailer carried by a serial frame, written/read little-endian (the fixed
// header is big-endian; the trailer is not part of that header). Pinned by the KAT.
inline std::array<std::byte, crc_trailer_size> crc_trailer(std::span<const std::byte> header, std::span<const std::byte> payload) noexcept
{
    const std::uint32_t crc = wire::crc32c(payload, wire::crc32c(header));
    return {std::byte(crc & 0xFFu), std::byte((crc >> 8) & 0xFFu), std::byte((crc >> 16) & 0xFFu), std::byte((crc >> 24) & 0xFFu)};
}

inline std::uint32_t read_trailer_le(std::span<const std::byte> t) noexcept
{
    return std::to_integer<std::uint32_t>(t[0]) | (std::to_integer<std::uint32_t>(t[1]) << 8) | (std::to_integer<std::uint32_t>(t[2]) << 16) |
            (std::to_integer<std::uint32_t>(t[3]) << 24);
}

// A self-resyncing CRC32C frame decorator between a serial read-loop and stream_inbound::feed. It
// scans for the magic anchor (0x56 0x50) to align; on a CRC-verified frame it emits the
// header+payload through on_match, and on a corrupt frame it drops the frame, advances one byte
// past the current magic, rescans, and raises a non-fatal crc_mismatch — it never closes the link.
class crc_serial_inbound
{
public:
    using emit = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
    using drop = plexus::detail::move_only_function<void(wire::close_cause)>;

    explicit crc_serial_inbound(std::size_t max_payload = wire::k_max_reassembler_payload_bytes) noexcept
            : m_max_payload(max_payload)
    {
    }

    void on_match(emit cb)
    {
        m_on_match = std::move(cb);
    }
    void on_drop(drop cb)
    {
        m_on_drop = std::move(cb);
    }

    void feed(std::span<const std::byte> bytes)
    {
        m_buf.insert(m_buf.end(), bytes.begin(), bytes.end());
        drain();
        compact();
    }

private:
    // Advance through every fully-arrived frame, resyncing on the magic anchor between them. The
    // await is bounded by max_payload, so a lying length cannot drive an unbounded wait.
    void drain()
    {
        for(;;)
        {
            if(!align_to_magic())
                return;
            const auto step = try_one_frame();
            if(step == step::need_more)
                return;
        }
    }

    enum class step : std::uint8_t
    {
        consumed,
        dropped,
        need_more
    };

    // need_more while the header or the declared frame+trailer span has not all arrived; consumed
    // on a CRC match; dropped (resync one byte) on a lying length or a failed trailer.
    step try_one_frame()
    {
        auto at = std::span<const std::byte>{m_buf}.subspan(m_pos);
        if(at.size() < wire::header_size)
            return step::need_more;
        const auto hdr = wire::decode_header(at);
        if(!hdr || hdr->payload_len > m_max_payload)
            return resync();
        const std::size_t frame_len = wire::header_size + hdr->payload_len;
        if(at.size() < frame_len + crc_trailer_size)
            return step::need_more;
        return verify(at, hdr->payload_len, frame_len);
    }

    step verify(std::span<const std::byte> at, std::size_t payload_len, std::size_t frame_len)
    {
        const auto header  = at.subspan(0, wire::header_size);
        const auto payload = at.subspan(wire::header_size, payload_len);
        const auto trailer = at.subspan(frame_len, crc_trailer_size);
        if(read_trailer_le(trailer) != wire::crc32c(payload, wire::crc32c(header)))
            return resync();
        if(m_on_match)
            m_on_match(at.subspan(0, frame_len));
        m_pos += frame_len + crc_trailer_size;
        return step::consumed;
    }

    // Advance exactly one byte past the magic so the next scan finds the following anchor (the
    // bounded resync), and raise the non-fatal signal.
    step resync()
    {
        ++m_pos;
        if(m_on_drop)
            m_on_drop(wire::close_cause::crc_mismatch);
        return step::dropped;
    }

    // Slide m_pos to the next magic anchor, discarding leading garbage. Keep at most a trailing
    // magic_byte_0 in case its pair arrives next feed, so junk between frames is bounded to <= 1 byte.
    bool align_to_magic()
    {
        const auto buf = std::span<const std::byte>{m_buf};
        while(m_pos + 1 < buf.size())
        {
            if(buf[m_pos] == wire::magic_byte_0 && buf[m_pos + 1] == wire::magic_byte_1)
                return true;
            ++m_pos;
        }
        if(m_pos < buf.size() && buf[m_pos] != wire::magic_byte_0)
            ++m_pos;
        return false;
    }

    // Reclaim the consumed/discarded prefix so the buffer footprint stays bounded by one in-flight
    // frame plus a feed chunk.
    void compact()
    {
        if(m_pos == 0)
            return;
        m_buf.erase(m_buf.begin(), m_buf.begin() + static_cast<std::ptrdiff_t>(m_pos));
        m_pos = 0;
    }

    std::size_t m_max_payload;
    std::size_t m_pos{0};
    std::vector<std::byte> m_buf;
    emit m_on_match;
    drop m_on_drop;
};

}

#endif
