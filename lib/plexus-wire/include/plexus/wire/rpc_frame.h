#ifndef HPP_GUARD_PLEXUS_WIRE_RPC_FRAME_H
#define HPP_GUARD_PLEXUS_WIRE_RPC_FRAME_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/rpc_status.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include <optional>

namespace plexus::wire {

struct rpc_request_decode_result
{
    bidirectional_header       header;
    std::span<const std::byte> param_data;
};

struct rpc_response_decode_result
{
    bidirectional_header       header;
    rpc_status                 status;
    std::span<const std::byte> return_data;
};

// Decode cutoff for the response status byte. The retained enumerators are
// sparse (6, 7, 9-17, 19 are reserved gaps with no defined value), so a bare
// "<= highest" range check could yield an undefined enumerator. The switch
// enumerates ONLY the nine defined values; an out-of-range byte or a reserved
// in-range gap matches no case, returns false, and the frame is rejected — so no
// undefined rpc_status enumerator is ever constructed.
inline bool is_defined_rpc_status(std::uint8_t byte) noexcept
{
    switch(static_cast<rpc_status>(byte))
    {
        case rpc_status::success:
        case rpc_status::error:
        case rpc_status::timeout:
        case rpc_status::cancelled:
        case rpc_status::no_handler:
        case rpc_status::deserialize_failed:
        case rpc_status::topic_not_found:
        case rpc_status::peer_disconnected:
        case rpc_status::rpc_response_orphan: return true;
    }
    return false;
}

inline std::vector<std::byte> encode_rpc_request(const bidirectional_header &hdr,
                                                 std::span<const std::byte>  param_data)
{
    return encode_bidirectional(hdr, param_data);
}

inline std::optional<rpc_request_decode_result>
decode_rpc_request(std::span<const std::byte> payload)
{
    auto decoded = decode_bidirectional(payload);
    if(!decoded)
        return std::nullopt;

    return rpc_request_decode_result{.header = decoded->header, .param_data = decoded->data};
}

inline std::vector<std::byte> encode_rpc_response(const bidirectional_header &hdr,
                                                  rpc_status                  status,
                                                  std::span<const std::byte>  return_data)
{
    std::vector<std::byte> combined(1 + return_data.size());
    combined[0] = static_cast<std::byte>(static_cast<std::uint8_t>(status));
    if(!return_data.empty())
        std::memcpy(combined.data() + 1, return_data.data(), return_data.size());

    return encode_bidirectional(hdr, combined);
}

inline std::optional<rpc_response_decode_result>
decode_rpc_response(std::span<const std::byte> payload)
{
    auto decoded = decode_bidirectional(payload);
    if(!decoded)
        return std::nullopt;

    if(decoded->data.empty())
        return std::nullopt;

    auto status_byte = static_cast<std::uint8_t>(decoded->data[0]);
    if(!is_defined_rpc_status(status_byte))
        return std::nullopt;

    return rpc_response_decode_result{.header      = decoded->header,
                                      .status      = static_cast<rpc_status>(status_byte),
                                      .return_data = decoded->data.subspan(1)};
}

// Zero-alloc dispatch overloads: frame into a caller-owned buffer reused across
// calls (the forwarder's hot path). The allocating returns above stay for
// one-shot callers.
inline void encode_rpc_request_into(std::vector<std::byte> &out, const bidirectional_header &hdr,
                                    std::span<const std::byte> param_data)
{
    encode_bidirectional_into(out, hdr, param_data);
}

inline void encode_rpc_response_into(std::vector<std::byte> &out, const bidirectional_header &hdr,
                                     rpc_status status, std::span<const std::byte> return_data)
{
    out.resize(bidirectional_header_size + 1 + return_data.size());
    writer w{out};

    w.u8(static_cast<uint8_t>(hdr.source));
    w.u64(hdr.sequence);
    w.u64(hdr.topic_hash);
    w.u64(hdr.type_hash_1);
    w.u64(hdr.type_hash_2);
    w.u64(hdr.correlation_id);
    w.u8(static_cast<uint8_t>(status));
    w.bytes(return_data);
}

}

#endif
