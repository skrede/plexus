#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_INPUT_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_INPUT_H

#include <span>
#include <cstddef>
#include <cstdint>
#include <algorithm>

class fuzz_input
{
public:
    explicit fuzz_input(std::span<const std::uint8_t> bytes) noexcept
            : m_at(0)
            , m_bytes(bytes)
    {
    }

    bool exhausted() const noexcept
    {
        return m_at >= m_bytes.size();
    }

    std::uint8_t u8() noexcept
    {
        return exhausted() ? 0 : m_bytes[m_at++];
    }

    // Little-endian, zero-padded once the input runs out.
    std::uint64_t uint(std::size_t width) noexcept
    {
        std::uint64_t value = 0;
        for(std::size_t i = 0; i < width && i < sizeof(value); ++i)
            value |= std::uint64_t{u8()} << (8 * i);
        return value;
    }

    // A width the input picks first, so small values and full-range ones are both cheap to reach.
    std::uint64_t any() noexcept
    {
        return uint(u8() % 9);
    }

    std::span<const std::uint8_t> take(std::size_t count) noexcept
    {
        const std::size_t n = std::min(count, m_bytes.size() - std::min(m_at, m_bytes.size()));
        const auto taken    = m_bytes.subspan(m_at, n);
        m_at += n;
        return taken;
    }

private:
    std::size_t m_at;
    std::span<const std::uint8_t> m_bytes;
};

#endif
