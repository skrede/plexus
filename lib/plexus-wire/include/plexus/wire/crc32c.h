#ifndef HPP_GUARD_PLEXUS_WIRE_CRC32C_H
#define HPP_GUARD_PLEXUS_WIRE_CRC32C_H

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

namespace detail {

// CRC-32C (Castagnoli, RFC 3720): polynomial 0x1EDC6F41, bit-reflected form
// 0x82F63B78 for the table-driven LSB-first variant.
inline constexpr std::uint32_t k_crc32c_poly_reflected = 0x82F63B78u;

constexpr std::array<std::uint32_t, 256> build_crc32c_table() noexcept
{
    std::array<std::uint32_t, 256> table{};
    for(std::uint32_t n = 0; n < 256u; ++n)
    {
        std::uint32_t c = n;
        for(int k = 0; k < 8; ++k)
            c = (c & 1u) ? (k_crc32c_poly_reflected ^ (c >> 1)) : (c >> 1);
        table[n] = c;
    }
    return table;
}

inline constexpr std::array<std::uint32_t, 256> k_crc32c_table = build_crc32c_table();

}

// Chains from a prior running value, so a multi-chunk record checksums without
// concatenation: crc32c(b) == crc32c(b.subspan(n), crc32c(b.first(n))).
inline std::uint32_t crc32c(std::span<const std::byte> data, std::uint32_t seed = 0u) noexcept
{
    std::uint32_t crc = ~seed;
    for(std::byte b : data)
    {
        const auto index = static_cast<std::uint8_t>(crc ^ std::to_integer<std::uint8_t>(b));
        crc              = detail::k_crc32c_table[index] ^ (crc >> 8);
    }
    return ~crc;
}

}

#endif
