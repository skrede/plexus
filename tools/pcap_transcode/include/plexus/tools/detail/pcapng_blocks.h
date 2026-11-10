#ifndef HPP_GUARD_PLEXUS_TOOLS_DETAIL_PCAPNG_BLOCKS_H
#define HPP_GUARD_PLEXUS_TOOLS_DETAIL_PCAPNG_BLOCKS_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::tools::detail {

// pcapng block + option codes (the on-disk format; every container field little-endian).
constexpr std::uint32_t k_shb_type         = 0x0A0D0D0Au;
constexpr std::uint32_t k_idb_type         = 0x00000001u;
constexpr std::uint32_t k_epb_type         = 0x00000006u;
constexpr std::uint32_t k_byte_order_magic = 0x1A2B3C4Du;
constexpr std::uint16_t k_opt_endofopt     = 0u;
constexpr std::uint16_t k_opt_comment      = 1u;
constexpr std::uint16_t k_opt_if_tsresol   = 9u;
constexpr std::uint16_t k_opt_epb_flags    = 2u;
constexpr std::uint16_t k_linktype_user0   = 147u; // DLT_USER0

// The pcapng spec maps the epb_flags direction field (low 2 bits) as inbound=1, outbound=2.
constexpr std::uint32_t k_dir_inbound  = 1u;
constexpr std::uint32_t k_dir_outbound = 2u;

inline void put_u16(std::vector<std::byte> &out, std::uint16_t v)
{
    out.push_back(static_cast<std::byte>(v & 0xff));
    out.push_back(static_cast<std::byte>((v >> 8) & 0xff));
}

inline void put_u32(std::vector<std::byte> &out, std::uint32_t v)
{
    for(std::uint32_t i = 0; i < 4; ++i)
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xff));
}

inline void put_u64(std::vector<std::byte> &out, std::uint64_t v)
{
    for(std::uint32_t i = 0; i < 8; ++i)
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xff));
}

inline void put_bytes(std::vector<std::byte> &out, std::span<const std::byte> bytes)
{
    out.insert(out.end(), bytes.begin(), bytes.end());
}

inline void pad4(std::vector<std::byte> &out)
{
    while(out.size() % 4 != 0)
        out.push_back(std::byte{0});
}

inline std::span<const std::byte> as_bytes(std::string_view s)
{
    return std::as_bytes(std::span{s.data(), s.size()});
}

// Append one option [code:u16][length:u16][value padded to 4]. length is the unpadded value size.
inline void put_option(std::vector<std::byte> &out, std::uint16_t code, std::span<const std::byte> value)
{
    put_u16(out, code);
    put_u16(out, static_cast<std::uint16_t>(value.size()));
    put_bytes(out, value);
    pad4(out);
}

inline void put_comment(std::vector<std::byte> &out, std::string_view text)
{
    put_option(out, k_opt_comment, as_bytes(text));
}

inline void put_endofopt(std::vector<std::byte> &out)
{
    put_u16(out, k_opt_endofopt);
    put_u16(out, 0);
}

// Wrap an already-built block body in [type][total_length][body][total_length]: the body is
// the bytes after the leading length, total_length counts the whole block including both
// length words, and the trailing word repeats it so a reader can walk the file backwards.
inline void seal_block(std::vector<std::byte> &out, std::uint32_t type, std::span<const std::byte> body)
{
    const auto total = static_cast<std::uint32_t>(body.size() + 12);
    put_u32(out, type);
    put_u32(out, total);
    put_bytes(out, body);
    put_u32(out, total);
}

}

#endif
