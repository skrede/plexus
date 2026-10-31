#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_REGION_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_REGION_H

#include "fuzz_input.h"

#include "plexus/shm/ring_header.h"

#include <new>
#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>

// A field of a byte image another process wrote; an image too short for it reads it as zero.
template<typename T>
T image_field(std::span<const std::uint8_t> image, std::size_t offset) noexcept
{
    T value{};
    if(offset + sizeof(T) <= image.size())
        std::memcpy(&value, image.data() + offset, sizeof(T));
    return value;
}

// A cache-line-aligned, zero-filled region allocated at exactly its size, so the sanitizer flags any access past it.
class fuzz_region
{
    struct aligned_free
    {
        void operator()(std::byte *p) const noexcept
        {
            ::operator delete(p, std::align_val_t{plexus::shm::k_cache_line});
        }
    };

public:
    explicit fuzz_region(std::size_t size)
            : m_size(size)
            , m_data(static_cast<std::byte *>(::operator new(std::max<std::size_t>(size, 1), std::align_val_t{plexus::shm::k_cache_line})))
    {
        std::memset(m_data.get(), 0, std::max<std::size_t>(size, 1));
    }

    // The image is truncated or zero-padded to the region's size.
    fuzz_region(std::span<const std::uint8_t> image, std::size_t size)
            : fuzz_region(size)
    {
        if(!image.empty() && size != 0)
            std::memcpy(m_data.get(), image.data(), std::min(image.size(), size));
    }

    std::span<std::byte> bytes() const noexcept
    {
        return {m_data.get(), m_size};
    }

    // Another process writing the shared region: each edit is one byte at an input-chosen offset.
    void tamper(fuzz_input &in, std::uint32_t edits) noexcept
    {
        for(std::uint32_t i = 0; i < edits && m_size != 0; ++i)
        {
            const std::size_t at = static_cast<std::size_t>(in.uint(2)) % m_size;
            m_data.get()[at]     = std::byte{in.u8()};
        }
    }

private:
    std::size_t m_size;
    std::unique_ptr<std::byte, aligned_free> m_data;
};

#endif
