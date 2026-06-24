#ifndef HPP_GUARD_PLEXUS_CALLER_H
#define HPP_GUARD_PLEXUS_CALLER_H

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/call_error.h"
#include "plexus/typed_codec.h"

#include "plexus/io/endpoint_seam.h"

#include "plexus/wire/varint.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/publisher_gid.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/caller_call.h"

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <system_error>
#include <string_view>

namespace plexus {

// deadline absent uses the forwarder's construction-time default.
struct call_options
{
    std::optional<std::chrono::nanoseconds> deadline{};
};

template<typename Sig, template<typename> class CReq, template<typename> class CRes>
class caller;

// CALLBACK-ONLY: there is no blocking/future form (a blocking call on the borrowed
// single-thread loop is a deadlock by construction).
//
// PROVIDER RESOLUTION: a call targets the first connection-order peer with a complete session.
// A call with NO connected provider does NOT hang/buffer/queue — the completion is POSTED
// carrying call_errc::no_provider.
//
// LIFETIME: a caller must NOT outlive its node. Dropping the handle does NOT cancel an in-flight
// call — a completion already handed to the forwarder runs to resolution (the asio convention:
// the operation owns its completion).
template<>
class caller<void, no_codec, no_codec>
{
public:
    using on_reply_fn = io::on_reply_fn;

    template<typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn)
            : m_seam(n.endpoint_seam_for())
            , m_fqn(fqn)
    {
    }

    template<typename Completion>
    void call(std::span<const std::byte> param, Completion &&completion)
    {
        dispatch(param, std::forward<Completion>(completion), std::nullopt);
    }

    template<typename Completion>
    void call(std::span<const std::byte> param, const call_options &opts, Completion &&completion)
    {
        dispatch(param, std::forward<Completion>(completion), opts.deadline);
    }

    caller(caller &&) noexcept            = default;
    caller &operator=(caller &&) noexcept = default;

    caller(const caller &)            = delete;
    caller &operator=(const caller &) = delete;

    ~caller() = default;

private:
    template<typename Completion>
    void dispatch(std::span<const std::byte> param, Completion &&completion, std::optional<std::chrono::nanoseconds> deadline)
    {
        detail::dispatch_bytes_call(m_seam, m_fqn, param, std::forward<Completion>(completion), deadline);
    }

    io::endpoint_seam m_seam{};
    std::string m_fqn;
};

// An encode/decode adaptation around the bytes caller. The completion adapter maps the wire
// reply: success decodes Res via CRes (a decode failure → deserialize_failed); a well-formed
// varint error-leg reconstructs the provider's error VALUE under provider_category (value
// preserved, category erased); an empty/malformed error-leg falls back to from_rpc_status.
template<typename Res, typename Req, template<typename> class CReq, template<typename> class CRes>
    requires typed_codec<CReq<Req>> && typed_codec<CRes<Res>>
class caller<Res(Req), CReq, CRes>
{
public:
    using on_reply_fn    = io::on_reply_fn;
    using request_codec  = CReq<Req>;
    using response_codec = CRes<Res>;

    template<typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn, request_codec req_codec = {}, response_codec res_codec = {})
            : m_seam(n.endpoint_seam_for())
            , m_req_codec(std::move(req_codec))
            , m_res_codec(std::move(res_codec))
            , m_fqn(fqn)
    {
    }

    template<typename Completion>
    void call(const Req &request, Completion &&completion)
    {
        dispatch(request, std::forward<Completion>(completion), std::nullopt);
    }

    template<typename Completion>
    void call(const Req &request, const call_options &opts, Completion &&completion)
    {
        dispatch(request, std::forward<Completion>(completion), opts.deadline);
    }

    caller(caller &&) noexcept            = default;
    caller &operator=(caller &&) noexcept = default;

    caller(const caller &)            = delete;
    caller &operator=(const caller &) = delete;

    ~caller() = default;

private:
    template<typename Completion>
    void dispatch(const Req &request, Completion &&completion, std::optional<std::chrono::nanoseconds> deadline)
    {
        const wire_bytes<> encoded = m_req_codec.encode(request);
        detail::dispatch_typed_call<Res>(m_seam, m_fqn, static_cast<std::span<const std::byte>>(encoded), m_res_codec, std::forward<Completion>(completion), deadline);
    }

    io::endpoint_seam m_seam{};
    request_codec m_req_codec;
    response_codec m_res_codec;
    std::string m_fqn;
};

}

#endif
