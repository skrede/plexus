#ifndef HPP_GUARD_PLEXUS_WIRE_CURSOR_H
#define HPP_GUARD_PLEXUS_WIRE_CURSOR_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/varint.h"
#include "plexus/wire/length_prefixed.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace plexus::wire {

// Sequential big-endian field writer over a pre-sized byte region. The caller
// resize()s / reserves the destination buffer to the frame's fixed size and the
// writer fills it field-by-field at an auto-advancing cursor, so the no-hot-path-
// allocation contract of the codecs' `_into` writers is preserved (writing into a
// buffer the caller already grew). bytes() copies an opaque blob verbatim. The
// writer wraps the fixed-width detail::write_uNN primitives; it never grows the
// region, so the cursor only ever advances within the caller-established bounds.
class writer
{
public:
    explicit writer(std::span<std::byte> region) noexcept : m_region(region) {}
    explicit writer(std::vector<std::byte> &out) noexcept : m_region(out) {}

    void u8(std::uint8_t v) noexcept
    {
        detail::write_u8(m_region.data() + m_offset, v);
        m_offset += sizeof(std::uint8_t);
    }

    void u16(std::uint16_t v) noexcept
    {
        detail::write_u16(m_region.data() + m_offset, v);
        m_offset += sizeof(std::uint16_t);
    }

    void u32(std::uint32_t v) noexcept
    {
        detail::write_u32(m_region.data() + m_offset, v);
        m_offset += sizeof(std::uint32_t);
    }

    void u64(std::uint64_t v) noexcept
    {
        detail::write_u64(m_region.data() + m_offset, v);
        m_offset += sizeof(std::uint64_t);
    }

    void bytes(std::span<const std::byte> blob) noexcept
    {
        if(!blob.empty())
            std::memcpy(m_region.data() + m_offset, blob.data(), blob.size());
        m_offset += blob.size();
    }

    [[nodiscard]] std::size_t offset() const noexcept { return m_offset; }

private:
    std::span<std::byte> m_region;
    std::size_t          m_offset{0};
};

// Sequential big-endian field reader over an untrusted byte span. Each fixed-width
// read checks remaining() >= width ONCE before reading; on underrun the reader
// latches a failed state (ok() == false) and every later read is a no-op returning
// a zero sentinel, so a decode body reads its whole field list then tests ok() once.
// This per-read bound is strictly stronger than the codecs' former single up-front
// size gate: it additionally catches a truncation BETWEEN fields, yet stays byte-
// identical for well-formed frames. varint() / length_prefixed() DELEGATE to the
// validated read_varint / read_length_prefixed guards (the LEB128 cap, the
// pre-multiply overflow and no-wrap checks live there, never reimplemented here).
class reader
{
public:
    explicit reader(std::span<const std::byte> data) noexcept : m_data(data) {}

    [[nodiscard]] bool ok() const noexcept { return m_ok; }
    [[nodiscard]] std::size_t consumed() const noexcept { return m_offset; }
    [[nodiscard]] std::size_t remaining() const noexcept { return m_data.size() - m_offset; }

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

    template <typename UIntT>
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

    std::span<const std::byte> m_data;
    std::size_t                m_offset{0};
    bool                       m_ok{true};
};

}

#endif
