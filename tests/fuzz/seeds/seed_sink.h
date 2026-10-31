#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_SEEDS_SEED_SINK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_SEEDS_SEED_SINK_H

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <initializer_list>

using seed     = std::vector<std::byte>;
using seed_set = std::vector<seed>;

inline void append(seed &out, std::span<const std::byte> bytes)
{
    out.insert(out.end(), bytes.begin(), bytes.end());
}

inline void append(seed &out, std::initializer_list<std::uint8_t> bytes)
{
    for(const std::uint8_t b : bytes)
        out.push_back(std::byte{b});
}

inline std::uint8_t lo(std::size_t v) noexcept
{
    return static_cast<std::uint8_t>(v);
}

inline std::uint8_t hi(std::size_t v) noexcept
{
    return static_cast<std::uint8_t>(v >> 8);
}

inline std::span<const std::byte> text(std::string_view s) noexcept
{
    return std::as_bytes(std::span<const char>{s.data(), s.size()});
}

inline seed prefixed(std::initializer_list<std::uint8_t> prefix, std::span<const std::byte> body)
{
    seed out;
    append(out, prefix);
    append(out, body);
    return out;
}

seed_set crc_serial_seeds();
seed_set data_frame_seeds();
seed_set rpc_frame_seeds();
seed_set handshake_frame_seeds();
seed_set subscribe_frame_seeds();
seed_set fetch_latched_frame_seeds();
seed_set heartbeat_frame_seeds();
seed_set topic_declaration_frame_seeds();
seed_set companion_frame_seeds();
seed_set announcement_frame_seeds();
seed_set peer_report_frame_seeds();
seed_set forwarded_frame_seeds();
seed_set frame_reassembler_seeds();
seed_set reassembler_seeds();
seed_set varint_seeds();
seed_set key_pattern_seeds();
seed_set ring_attach_seeds();
seed_set udp_envelope_seeds();
seed_set stream_inbound_seeds();

#endif
