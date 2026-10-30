#ifndef HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H
#define HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H

#include "plexus/stream/serial_frame.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::stream {

// A self-resyncing CRC32C frame decorator between a serial read-loop and stream_inbound::feed. It emits each
// verified frame through on_match; on a refused frame it resyncs and raises a non-fatal crc_mismatch, never closing the link.
class crc_serial_inbound
{
    enum class step : std::uint8_t
    {
        consumed,
        dropped,
        need_more
    };

public:
    using emit = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
    using drop = plexus::detail::move_only_function<void(wire::close_cause)>;

    explicit crc_serial_inbound(std::size_t max_payload = wire::k_max_reassembler_payload_bytes) noexcept
            : m_pos(0)
            , m_max_payload(max_payload)
    {
    }

    void on_match(emit cb)
    {
        m_on_match_cb = std::move(cb);
    }

    void on_drop(drop cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    void feed(std::span<const std::byte> bytes)
    {
        m_buf.insert(m_buf.end(), bytes.begin(), bytes.end());
        drain();
        compact();
    }

private:
    std::size_t m_pos;
    std::size_t m_max_payload;
    std::vector<std::byte> m_buf;
    drop m_on_drop_cb;
    emit m_on_match_cb;

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
        if(read_crc_le(trailer) != wire::crc32c(payload, wire::crc32c(header)))
            return resync();
        if(m_on_match_cb)
            m_on_match_cb(at.subspan(0, frame_len));
        m_pos += frame_len + crc_trailer_size;
        return step::consumed;
    }

    step resync()
    {
        ++m_pos;
        if(m_on_drop_cb)
            m_on_drop_cb(wire::close_cause::crc_mismatch);
        return step::dropped;
    }

    // Keeps at most a trailing magic_byte_0, in case its pair arrives with the next feed.
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

    void compact()
    {
        if(m_pos == 0)
            return;
        m_buf.erase(m_buf.begin(), m_buf.begin() + static_cast<std::ptrdiff_t>(m_pos));
        m_pos = 0;
    }
};

}

#endif
