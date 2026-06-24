#ifndef HPP_GUARD_PLEXUS_DETAIL_CALLER_CALL_H
#define HPP_GUARD_PLEXUS_DETAIL_CALLER_CALL_H

#include "plexus/expected.h"
#include "plexus/call_error.h"
#include "plexus/publisher_gid.h"

#include "plexus/io/endpoint_seam.h"

#include "plexus/wire/varint.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus {

// Only provider_identity and reception_timestamp are genuine; the response leg does not echo the
// rest, so they stay absent (0 / false) rather than carrying a fabricated value.
struct reply_info
{
    std::optional<publisher_gid> provider_identity{};
    std::uint64_t source_timestamp{};
    std::uint64_t reception_timestamp{};
    bool from_intra_process{};
};

// bytes views the response frame's return bytes — valid for the completion invocation only, never
// retained.
struct reply
{
    std::span<const std::byte> bytes;
    reply_info info;
};

namespace detail {

// An absent wire status is the no_provider verdict that never rode the wire; success carries the
// return bytes plus provider attribution; any other status maps through from_rpc_status.
template<typename Completion>
void dispatch_bytes_call(const io::endpoint_seam &seam, std::string_view fqn, std::span<const std::byte> param, Completion &&completion,
                         std::optional<std::chrono::nanoseconds> deadline)
{
    seam.call(
            seam.ctx, fqn, param,
            [completion = std::forward<Completion>(completion)](std::optional<wire::rpc_status> status, std::span<const std::byte> bytes,
                                                                const std::optional<publisher_gid> &provider) mutable
            {
                if(!status)
                {
                    completion(expected<reply, std::error_code>{unexpect, make_error_code(call_errc::no_provider)});
                    return;
                }
                if(*status == wire::rpc_status::success)
                {
                    reply r{bytes, reply_info{provider, 0, wire::now_timestamp_ns(), false}};
                    completion(expected<reply, std::error_code>{std::move(r)});
                    return;
                }
                completion(expected<reply, std::error_code>{unexpect, make_error_code(from_rpc_status(*status))});
            },
            deadline);
}

template<typename Res, typename ResponseCodec>
expected<Res, std::error_code> resolve_typed_reply(std::optional<wire::rpc_status> status, std::span<const std::byte> bytes, ResponseCodec &res_codec)
{
    using result = expected<Res, std::error_code>;
    if(!status)
        return result{unexpect, make_error_code(call_errc::no_provider)};
    if(*status == wire::rpc_status::success)
    {
        Res value{};
        if(res_codec.decode(bytes, value))
            return result{std::move(value)};
        return result{unexpect, make_error_code(call_errc::deserialize_failed)};
    }
    // A well-formed varint error-leg reconstructs the provider's error value under
    // provider_category (value preserved, category erased); an empty or malformed leg falls back to
    // from_rpc_status, so a hostile varint never crashes nor half-decodes.
    if(*status == wire::rpc_status::error)
    {
        std::size_t consumed = 0;
        if(const auto provider_value = wire::read_varint(bytes, consumed); provider_value && consumed == bytes.size())
            return result{unexpect, std::error_code{static_cast<int>(*provider_value), provider_category()}};
    }
    return result{unexpect, make_error_code(from_rpc_status(*status))};
}

// The codec is copied into the completion so the closure stays self-contained against a mid-flight
// move of the handle.
template<typename Res, typename ResponseCodec, typename Completion>
void dispatch_typed_call(const io::endpoint_seam &seam, std::string_view fqn, std::span<const std::byte> encoded, ResponseCodec res_codec, Completion &&completion,
                         std::optional<std::chrono::nanoseconds> deadline)
{
    seam.call(
            seam.ctx, fqn, encoded,
            [completion = std::forward<Completion>(completion), res_codec = std::move(res_codec)](std::optional<wire::rpc_status> status, std::span<const std::byte> bytes,
                                                                                                  const std::optional<publisher_gid> &) mutable
            { completion(resolve_typed_reply<Res>(status, bytes, res_codec)); },
            deadline);
}

}
}

#endif
