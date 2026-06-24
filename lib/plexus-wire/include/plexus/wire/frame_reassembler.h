#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_REASSEMBLER_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_REASSEMBLER_H

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <memory>
#include <vector>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace plexus::wire {

// Owning, shareable wire-payload buffer held behind a shared_ptr so the receive seam can hand
// a zero-copy owning handle to the consumer. Implicitly converts to std::span so the
// wire::decode_* call sites consume it unchanged.
class shared_bytes
{
public:
    shared_bytes() = default;

    // Implicit by design: an unambiguous ownership transfer of an owning byte vector.
    shared_bytes(std::vector<std::byte> bytes)
            : m_owner(std::make_shared<const std::vector<std::byte>>(std::move(bytes)))
    {
    }

    const std::byte *data() const noexcept
    {
        return m_owner ? m_owner->data() : nullptr;
    }
    std::size_t size() const noexcept
    {
        return m_owner ? m_owner->size() : 0u;
    }
    bool empty() const noexcept
    {
        return size() == 0u;
    }

    const std::byte *begin() const noexcept
    {
        return data();
    }
    const std::byte *end() const noexcept
    {
        return data() + size();
    }

    operator std::span<const std::byte>() const noexcept
    {
        return {data(), size()};
    }

    // Type-erased owner handle the receive seam binds a wire_bytes view to.
    std::shared_ptr<const void> owner() const noexcept
    {
        return m_owner;
    }

    friend bool operator==(const shared_bytes &a, std::span<const std::byte> b) noexcept
    {
        return std::ranges::equal(std::span<const std::byte>(a), b);
    }

private:
    std::shared_ptr<const std::vector<std::byte>> m_owner;
};

struct complete_frame
{
    frame_header header;
    // The FULL header-on frame (serialized header followed by its inner bytes), one contiguous
    // owner, so the receive seam carries it straight to delivery without re-prepending.
    shared_bytes payload;
};

enum class feed_error : uint8_t
{
    none,
    invalid_magic,
    payload_too_large,
    buffer_overflow,
    no_progress
};

struct feed_result
{
    std::vector<complete_frame> frames;
    feed_error error{feed_error::none};
};

class frame_reassembler
{
    enum class state : uint8_t
    {
        reading_header,
        reading_payload
    };

public:
    explicit frame_reassembler(std::size_t max_payload_size = k_max_reassembler_payload_bytes, std::size_t buffered_bytes_cap = k_max_reassembler_payload_bytes + header_size)
            : m_max_payload_size{max_payload_size}
            , m_buffered_bytes_cap{buffered_bytes_cap}
    {
    }

    // NOLINTNEXTLINE(readability-function-size)
    feed_result feed(std::span<const std::byte> data)
    {
        compact();
        m_buffer.insert(m_buffer.end(), data.begin(), data.end());

        feed_result result;

        if(live_bytes() > m_buffered_bytes_cap)
        {
            result.error = feed_error::buffer_overflow;
            reset();
            return result;
        }

        for(;;)
        {
            auto remaining = std::span<const std::byte>{m_buffer}.subspan(m_consumed);

            if(m_state == state::reading_header)
            {
                if(remaining.size() < header_size)
                    break;

                auto hdr = decode_header(remaining);
                if(!hdr)
                {
                    result.error = feed_error::invalid_magic;
                    reset();
                    return result;
                }

                m_pending_header = *hdr;
                m_consumed += header_size;
                m_state = state::reading_payload;
            }

            if(m_state == state::reading_payload)
            {
                if(m_pending_header.payload_len > m_max_payload_size)
                {
                    result.error = feed_error::payload_too_large;
                    reset();
                    return result;
                }

                remaining = std::span<const std::byte>{m_buffer}.subspan(m_consumed);

                if(remaining.size() < m_pending_header.payload_len)
                    break;

                auto payload_span = remaining.subspan(0, m_pending_header.payload_len);

                std::vector<std::byte> framed(header_size + m_pending_header.payload_len);
                auto header_bytes = encode_header(m_pending_header);
                writer{std::span<std::byte>{framed}}.bytes(header_bytes);
                if(!payload_span.empty())
                    std::memcpy(framed.data() + header_size, payload_span.data(), payload_span.size());

                result.frames.push_back(complete_frame{.header = m_pending_header, .payload = shared_bytes{std::move(framed)}});

                m_consumed += m_pending_header.payload_len;
                m_state = state::reading_header;
            }
        }

        return result;
    }

    void reset()
    {
        m_buffer.clear();
        m_consumed       = 0;
        m_state          = state::reading_header;
        m_pending_header = {};
    }

    std::size_t buffered_bytes() const
    {
        return live_bytes();
    }

    bool frame_in_progress() const
    {
        return m_state == state::reading_payload || live_bytes() != 0;
    }

    // The declared payload length once the header is decoded; nullopt while the size is still
    // unknown (a partial header), so the no-progress timer keys off declared size, not buffered
    // bytes.
    std::optional<std::size_t> pending_payload_len() const
    {
        if(m_state == state::reading_payload)
            return m_pending_header.payload_len;
        return std::nullopt;
    }

private:
    std::size_t live_bytes() const
    {
        return m_buffer.size() - m_consumed;
    }

    // Reclaim the consumed prefix once it dominates the buffer, amortizing the front erase
    // across feeds instead of an O(n) tail memmove on every frame boundary.
    void compact()
    {
        if(m_consumed != 0 && m_consumed >= m_buffer.size() - m_consumed)
        {
            m_buffer.erase(m_buffer.begin(), m_buffer.begin() + static_cast<std::ptrdiff_t>(m_consumed));
            m_consumed = 0;
        }
    }

    state m_state{state::reading_header};
    std::size_t m_max_payload_size;
    std::size_t m_buffered_bytes_cap;
    std::size_t m_consumed{0};
    frame_header m_pending_header{};
    std::vector<std::byte> m_buffer;
};

}

#endif
