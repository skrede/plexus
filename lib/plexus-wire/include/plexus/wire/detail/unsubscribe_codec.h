#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_UNSUBSCRIBE_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_UNSUBSCRIBE_CODEC_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/subscribe.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

inline std::vector<std::byte> encode_unsubscribe_request(const unsubscribe_request &req)
{
    std::vector<std::byte> buf(detail::unsubscribe_request_size);
    wire::detail::write_u64(buf.data(), req.topic_hash);
    return buf;
}

inline std::optional<unsubscribe_request> decode_unsubscribe_request(std::span<const std::byte> payload)
{
    if(payload.size() < detail::unsubscribe_request_size)
        return std::nullopt;
    return unsubscribe_request{.topic_hash = wire::detail::read_u64(payload.data())};
}

inline std::vector<std::byte> encode_unsubscribe_response(const unsubscribe_response &resp)
{
    std::vector<std::byte> buf(detail::unsubscribe_response_size);
    wire::detail::write_u64(buf.data(), resp.topic_hash);
    wire::detail::write_u8(buf.data() + 8, static_cast<uint8_t>(resp.status));
    return buf;
}

inline std::optional<unsubscribe_response> decode_unsubscribe_response(std::span<const std::byte> payload)
{
    if(payload.size() < detail::unsubscribe_response_size)
        return std::nullopt;
    return unsubscribe_response{.topic_hash = wire::detail::read_u64(payload.data()), .status = static_cast<unsubscribe_status>(wire::detail::read_u8(payload.data() + 8))};
}
}

#endif
