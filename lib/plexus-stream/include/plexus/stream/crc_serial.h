#ifndef HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H
#define HPP_GUARD_PLEXUS_STREAM_CRC_SERIAL_H

#include "plexus/stream/serial_frame.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/close_cause.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::stream {

// A self-resyncing CRC32C frame decorator between a serial read-loop and stream_inbound::feed. It emits each verified
// frame as a wire frame through on_match. A header whose CRC fails is refused as soon as it is buffered, before its
// payload_len is trusted; on a refused frame it resyncs and raises a non-fatal crc_mismatch, never closing the link.
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
            if(!align_to_format_id())
                return;
            const auto step = try_one_frame();
            if(step == step::need_more)
                return;
        }
    }

    step try_one_frame()
    {
        const auto at = std::span<std::byte>{m_buf}.subspan(m_pos);
        if(at.size() < serial_header_size)
            return step::need_more;
        if(!serial_header_intact(at))
            return resync();
        const std::uint64_t payload_len = serial_payload_len(at);
        if(payload_len > m_max_payload)
            return resync();
        const std::size_t frame_len = serial_header_size + static_cast<std::size_t>(payload_len);
        if(at.size() < frame_len + crc_trailer_size)
            return step::need_more;
        return verify(at, frame_len);
    }

    step verify(std::span<std::byte> at, std::size_t frame_len)
    {
        const auto header  = at.first(serial_header_size);
        const auto payload = at.subspan(serial_header_size, frame_len - serial_header_size);
        if(read_crc_le(at.subspan(frame_len, crc_trailer_size)) != wire::crc32c(payload, wire::crc32c(header)))
            return resync();
        const auto frame = wire_frame_in_place(at.first(frame_len));
        if(m_on_match_cb)
            m_on_match_cb(frame);
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

    // Keeps at most a trailing first format-id byte, in case its pair arrives with the next feed.
    bool align_to_format_id()
    {
        const auto buf = std::span<const std::byte>{m_buf};
        while(m_pos + 1 < buf.size())
        {
            if(peek_serial_format(buf.subspan(m_pos)))
                return true;
            ++m_pos;
        }
        if(m_pos < buf.size() && buf[m_pos] != k_serial_format_id[0])
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
