#ifndef HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H
#define HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::stream {

inline constexpr std::size_t crc_trailer_size = 4;

// The CRC32C integrity trailer carried by a serial frame, written/read little-endian.
// It is the decorator's own framing concern ABOVE the byte-stable wire (the fixed
// header is big-endian; the trailer is not part of that header), so its byte order is
// self-consistent between this writer and the inbound reader, pinned by the KAT.
[[nodiscard]] inline std::array<std::byte, crc_trailer_size> crc_trailer(std::span<const std::byte> header, std::span<const std::byte> payload) noexcept
{
    const std::uint32_t crc = wire::crc32c(payload, wire::crc32c(header));
    return {std::byte(crc & 0xFFu), std::byte((crc >> 8) & 0xFFu), std::byte((crc >> 16) & 0xFFu), std::byte((crc >> 24) & 0xFFu)};
}

[[nodiscard]] inline std::uint32_t read_trailer_le(std::span<const std::byte> t) noexcept
{
    return std::to_integer<std::uint32_t>(t[0]) | (std::to_integer<std::uint32_t>(t[1]) << 8) | (std::to_integer<std::uint32_t>(t[2]) << 16) |
            (std::to_integer<std::uint32_t>(t[3]) << 24);
}

// The self-resyncing CRC32C frame decorator that sits between a serial read-loop and
// stream_inbound::feed. It owns the byte boundary: it scans for the magic anchor
// (0x56 0x50) to align — this resync is genuinely NEW code, there is none in the tree
// (frame_reassembler resets-then-closes). On a CRC-verified frame it emits the clean
// header+payload bytes through on_match (the channel forwards them into stream_inbound
// exactly as the TCP read-loop does); on a corrupt frame it DROPS the frame, advances
// one byte past the current magic, rescans, and raises a NON-FATAL crc_mismatch through
// on_drop — it NEVER aborts or closes the link. Policy-free and exception-free so the
// MCU build reuses it on the -fno-exceptions floor.
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
    // Advance through every fully-arrived frame the buffer holds, resyncing on the
    // magic anchor between them. Returns when the buffer needs more bytes to make
    // progress (a partial header/payload at a valid anchor) — the await is bounded by
    // max_payload, so a lying length cannot drive an unbounded wait.
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

    // Verify the frame anchored at m_pos. need_more while the header or the declared
    // frame+trailer span has not all arrived; consumed (emit) on a CRC match; dropped
    // (resync one byte) on a lying length or a failed trailer — the link stays up.
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

    // Drop the frame at m_pos and advance EXACTLY one byte past its magic so the next
    // scan finds the following anchor — the bounded resync. Raise the non-fatal signal.
    step resync()
    {
        ++m_pos;
        if(m_on_drop)
            m_on_drop(wire::close_cause::crc_mismatch);
        return step::dropped;
    }

    // Slide m_pos to the next magic anchor, discarding leading garbage. False (await
    // more) when no anchor is present: keep at most a trailing magic_byte_0 in case the
    // second magic byte arrives next feed, so junk between frames is bounded to <= 1 byte.
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

    // Reclaim the consumed/discarded prefix so the live buffer never retains skipped
    // garbage — the buffer footprint stays bounded by one in-flight frame + a feed chunk.
    void compact()
    {
        if(m_pos == 0)
            return;
        m_buf.erase(m_buf.begin(), m_buf.begin() + static_cast<std::ptrdiff_t>(m_pos));
        m_pos = 0;
    }

    std::size_t            m_max_payload;
    std::size_t            m_pos{0};
    std::vector<std::byte> m_buf;
    emit                   m_on_match;
    drop                   m_on_drop;
};

}

#endif
