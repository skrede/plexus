#ifndef HPP_GUARD_PLEXUS_WIRE_READER_H
#define HPP_GUARD_PLEXUS_WIRE_READER_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/length_prefixed.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace plexus::wire {

// Sequential big-endian field reader over an untrusted span. Each fixed-width read
// checks remaining() >= width once; on underrun it latches ok() == false and every
// later read is a no-op returning a zero sentinel, so a decode body reads its whole
// field list then tests ok() once -- this per-read bound also catches a truncation
// between fields. varint() / length_prefixed() delegate to the validated read_varint
// / read_length_prefixed guards (the LEB128 cap and the pre-multiply overflow / no-
// wrap checks live there).
class reader
{
public:
    explicit reader(std::span<const std::byte> data) noexcept
            : m_data(data)
    {
    }

    [[nodiscard]] bool ok() const noexcept
    {
        return m_ok;
    }
    [[nodiscard]] std::size_t consumed() const noexcept
    {
        return m_offset;
    }
    [[nodiscard]] std::size_t remaining() const noexcept
    {
        return m_data.size() - m_offset;
    }

    std::uint8_t u8() noexcept
    {
        if(!take(sizeof(std::uint8_t)))
            return 0;
        return detail::read_u8(m_data.data() + (m_offset - sizeof(std::uint8_t)));
    }

    std::uint16_t u16() noexcept
    {
        if(!take(sizeof(std::uint16_t)))
            return 0;
        return detail::read_u16(m_data.data() + (m_offset - sizeof(std::uint16_t)));
    }

    std::uint32_t u32() noexcept
    {
        if(!take(sizeof(std::uint32_t)))
            return 0;
        return detail::read_u32(m_data.data() + (m_offset - sizeof(std::uint32_t)));
    }

    std::uint64_t u64() noexcept
    {
        if(!take(sizeof(std::uint64_t)))
            return 0;
        return detail::read_u64(m_data.data() + (m_offset - sizeof(std::uint64_t)));
    }

    std::span<const std::byte> bytes(std::size_t n) noexcept
    {
        if(!take(n))
            return {};
        return m_data.subspan(m_offset - n, n);
    }

    void copy_to(std::byte *dst, std::size_t n) noexcept
    {
        auto view = bytes(n);
        if(m_ok && n != 0)
            std::memcpy(dst, view.data(), n);
    }

    std::optional<std::uint64_t> varint() noexcept
    {
        if(!m_ok)
            return std::nullopt;
        auto value = read_varint(m_data, m_offset);
        if(!value)
            m_ok = false;
        return value;
    }

    template<typename UIntT>
    std::span<const std::byte> length_prefixed(std::size_t element_size = 1) noexcept
    {
        if(!m_ok)
            return {};
        auto payload = read_length_prefixed<UIntT>(m_data, m_offset, element_size);
        if(!payload)
        {
            m_ok = false;
            return {};
        }
        return *payload;
    }

    [[nodiscard]] std::span<const std::byte> rest() const noexcept
    {
        return m_data.subspan(m_offset);
    }

private:
    std::span<const std::byte> m_data;
    std::size_t                m_offset{0};
    bool                       m_ok{true};

    bool take(std::size_t width) noexcept
    {
        if(!m_ok || remaining() < width)
        {
            m_ok = false;
            return false;
        }
        m_offset += width;
        return true;
    }
};

}

#endif
