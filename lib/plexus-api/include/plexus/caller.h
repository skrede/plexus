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

// deadline is optional because ABSENCE is meaningful: absent = use the forwarder's
// construction-time default, distinct from any concrete override.
struct call_options
{
    std::optional<std::chrono::nanoseconds> deadline{};
};

// The codec slots are template-template parameters (a codec FAMILY over one value type, not a
// finished codec); the response family defaults to the request family (the symmetric form).
// The bytes endpoint is the caller<void, no_codec, no_codec> specialization; caller<> selects
// it via the defaulted parameters (the defaults live in node.h's forward declaration, seen
// first). no_codec is a sentinel family, never instantiated.
template<typename Sig, template<typename> class CReq, template<typename> class CRes>
class caller;

// The bytes calling endpoint. CALLBACK-ONLY — there is no blocking/future form (a blocking
// call on the borrowed single-thread loop is a deadlock by construction). The completion is
// void(expected<reply, std::error_code>).
//
// PROVIDER RESOLUTION (single-provider): a call targets the FIRST connection-order peer with a
// complete session (a node refuses a second LOCAL registration on one fqn, so in-process the
// provider is unique). A call with NO connected provider does NOT hang/buffer/queue — the
// completion is POSTED carrying call_errc::no_provider; a wrong-aim resolves no_handler through
// the per-call deadline path.
//
// LIFETIME: a caller must NOT outlive its node. Dropping the handle does NOT cancel an in-flight
// call — a completion already handed to the forwarder runs to resolution (the asio convention:
// the operation owns its completion). A moved-from handle is inert.
template<>
class caller<void, no_codec, no_codec>
{
public:
    // An ABSENT wire status is the facade no_provider verdict that never rode the wire.
    using on_reply_fn = io::on_reply_fn;

    template<typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn)
            : m_seam(n.endpoint_seam_for())
            , m_fqn(fqn)
    {
    }

    // Resolve the first connected provider and dispatch; on no provider, POST the no_provider
    // completion (never inline).
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
    void dispatch(std::span<const std::byte> param, Completion &&completion,
                  std::optional<std::chrono::nanoseconds> deadline)
    {
        detail::dispatch_bytes_call(m_seam, m_fqn, param, std::forward<Completion>(completion),
                                    deadline);
    }

    io::endpoint_seam m_seam{};
    std::string       m_fqn;
};

// The typed calling endpoint: an encode/decode adaptation around the bytes caller. There is NO
// inproc fast path for RPC by design — a request/response always rides bytes.
//
// The completion adapter maps the wire reply: success decodes Res via CRes (a decode failure →
// deserialize_failed); a well-formed varint error-leg reconstructs the provider's error VALUE
// under provider_category (value preserved, category erased); an empty/MALFORMED error-leg
// falls back to from_rpc_status (the interop fallback — a hostile varint never crashes nor
// half-decodes).
template<typename Res, typename Req, template<typename> class CReq, template<typename> class CRes>
    requires typed_codec<CReq<Req>> && typed_codec<CRes<Res>>
class caller<Res(Req), CReq, CRes>
{
public:
    using on_reply_fn    = io::on_reply_fn;
    using request_codec  = CReq<Req>;
    using response_codec = CRes<Res>;

    template<typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn, request_codec req_codec = {},
           response_codec res_codec = {})
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
    void dispatch(const Req &request, Completion &&completion,
                  std::optional<std::chrono::nanoseconds> deadline)
    {
        const wire_bytes<> encoded = m_req_codec.encode(request);
        detail::dispatch_typed_call<Res>(
                m_seam, m_fqn, static_cast<std::span<const std::byte>>(encoded), m_res_codec,
                std::forward<Completion>(completion), deadline);
    }

    io::endpoint_seam m_seam{};
    request_codec     m_req_codec;
    response_codec    m_res_codec;
    std::string       m_fqn;
};

}

#endif
