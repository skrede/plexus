#ifndef HPP_GUARD_PLEXUS_WIRE_WRITER_H
#define HPP_GUARD_PLEXUS_WIRE_WRITER_H

#include "plexus/wire/byte_order.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace plexus::wire {

// Sequential big-endian field writer over a caller-pre-sized region: it fills the
// buffer at an auto-advancing cursor and never grows it, preserving the no-hot-path-
// allocation contract of the codecs' `_into` writers.
class writer
{
public:
    explicit writer(std::span<std::byte> region) noexcept
            : m_region(region)
    {
    }
    explicit writer(std::vector<std::byte> &out) noexcept
            : m_region(out)
    {
    }

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

    // LEB128-encode `value` at the cursor, byte-identical to write_varint (the caller
    // sizes the region for varint_size(value)).
    void varint(std::uint64_t value) noexcept
    {
        do
        {
            auto byte = static_cast<std::uint8_t>(value & 0x7Fu);
            value >>= 7u;
            if(value != 0)
                byte |= 0x80u;
            m_region[m_offset++] = static_cast<std::byte>(byte);
        } while(value != 0);
    }

    [[nodiscard]] std::size_t offset() const noexcept
    {
        return m_offset;
    }

private:
    std::span<std::byte> m_region;
    std::size_t          m_offset{0};
};

}

#endif
