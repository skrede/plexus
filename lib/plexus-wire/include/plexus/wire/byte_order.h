#ifndef HPP_GUARD_PLEXUS_WIRE_BYTE_ORDER_H
#define HPP_GUARD_PLEXUS_WIRE_BYTE_ORDER_H

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace plexus::wire::detail {

// C++20 constexpr stand-in for std::byteswap (C++23).
template<typename T>
constexpr T byteswap(T value) noexcept
{
    static_assert(std::is_unsigned_v<T>, "byteswap requires an unsigned type");
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "byteswap supports 16/32/64-bit widths");
    if constexpr(sizeof(T) == 2)
        return static_cast<T>((value >> 8) | (value << 8));
    else if constexpr(sizeof(T) == 4)
        return static_cast<T>((value >> 24) | ((value >> 8) & 0x0000FF00u) | ((value << 8) & 0x00FF0000u) | (value << 24));
    else
        return static_cast<T>(((value >> 56)) | ((value >> 40) & 0x000000000000FF00ull) | ((value >> 24) & 0x0000000000FF0000ull) | ((value >> 8) & 0x00000000FF000000ull) |
                              ((value << 8) & 0x000000FF00000000ull) | ((value << 24) & 0x0000FF0000000000ull) | ((value << 40) & 0x00FF000000000000ull) | ((value << 56)));
}

inline void write_u8(std::byte *dst, uint8_t val)
{
    *dst = std::byte{val};
}

inline uint8_t read_u8(const std::byte *src)
{
    return std::to_integer<uint8_t>(*src);
}

inline void write_u16(std::byte *dst, uint16_t val)
{
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    std::memcpy(dst, &val, sizeof(val));
}

inline uint16_t read_u16(const std::byte *src)
{
    uint16_t val;
    std::memcpy(&val, src, sizeof(val));
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    return val;
}

inline void write_u32(std::byte *dst, uint32_t val)
{
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    std::memcpy(dst, &val, sizeof(val));
}

inline uint32_t read_u32(const std::byte *src)
{
    uint32_t val;
    std::memcpy(&val, src, sizeof(val));
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    return val;
}

inline void write_u64(std::byte *dst, uint64_t val)
{
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    std::memcpy(dst, &val, sizeof(val));
}

inline uint64_t read_u64(const std::byte *src)
{
    uint64_t val;
    std::memcpy(&val, src, sizeof(val));
    if constexpr(std::endian::native == std::endian::little)
        val = byteswap(val);
    return val;
}

}

#endif
