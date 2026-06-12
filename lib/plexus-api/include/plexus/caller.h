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
#include "plexus/detail/function_traits.h"

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

// The wire+local attribution split for a reply, mirroring io::message_info.
// Populated honestly: a field carries data ONLY when the caller seam genuinely holds
// it, and a field the response leg does not surface stays in its documented absent
// state rather than a fabricated value.
//
// provider_identity is std::optional<publisher_gid>: it is engaged with the RESOLVED
// provider node's node_id (the peer the call was deterministically targeted to), so
// attribution is genuine at node granularity. The endpoint_counter half is the
// documented absent state (0): the procedure provider's endpoint counter is not echoed
// on the rpc_response wire, so it is never fabricated.
//
// source_timestamp is the absent state (0): the rpc_response frame carries a
// frame-header timestamp, but the procedure_forwarder's on_response seam delivers only
// the status and the return bytes, so it is not fabricated here.
//
// reception_timestamp is receiver-stamped at completion from the codec's clock.
// from_intra_process is the absent state (false): the locality tier of the answering
// channel is not surfaced through the on_response seam.
struct reply_info
{
    std::optional<publisher_gid> provider_identity{};
    std::uint64_t                source_timestamp{};
    std::uint64_t                reception_timestamp{};
    bool                         from_intra_process{};
};

// The successful reply handed to the completion: a VIEW into the response frame's
// return bytes (valid for the completion invocation only — never retained) plus the
// reply_info attribution.
struct reply
{
    std::span<const std::byte> bytes;
    reply_info                 info;
};

// Per-call options, an extensible designated-initializer aggregate. deadline is
// std::optional because its ABSENCE is meaningful — absent means "use the forwarder's
// construction-time default deadline", a distinct state from any concrete override.
struct call_options
{
    std::optional<std::chrono::nanoseconds> deadline{};
};

// The calling endpoint family. The primary template names the typed endpoint
// (caller<Res(Req), CReq, CRes>); the bytes endpoint is the caller<void, void, void>
// specialization below — caller<> selects it via the defaulted parameters (the defaults
// live in node.h's forward declaration, seen first), so every bytes spelling keeps
// compiling.
template <typename Sig, typename CReq, typename CRes>
class caller;

// The bytes calling endpoint: the CONSTRUCTOR binds the node and fqn; the handle owns
// the call verb. The caller is CALLBACK-ONLY — there is no blocking/future form (a
// blocking call on the borrowed single-thread loop is a deadlock by construction; a
// consumer owning threads wraps the callback in a few lines). The completion is exactly
// void(plexus::expected<reply, std::error_code>): a success carries reply{bytes, info};
// every failure carries a call_errc.
//
// PROVIDER RESOLUTION (single-provider): a call targets the FIRST connection-order peer
// with a complete session. A node REFUSES a second LOCAL procedure registration on one
// fqn (procedure.h), so within a process the provider is unique; global multi-provider
// arbitration is structurally out of scope. A call with NO connected provider does NOT
// hang, buffer, or queue: the completion is POSTED on the borrowed executor carrying
// call_errc::no_provider. A wrong-aim (a peer that does not serve the fqn) resolves
// no_handler through the existing per-call deadline path.
//
// LIFETIME: a caller must NOT outlive its node. Dropping the handle is
// bookkeeping-only — it does NOT cancel an in-flight call: a completion already handed
// to the forwarder runs to its resolution (the asio convention — the operation owns its
// completion, not the initiating handle). Cancellation sugar is seeded, not invented
// here. A moved-from handle is inert.
template <>
class caller<void, void, void>
{
public:
    // The completion-side callback the node seam fans: the wire status (ENGAGED on a
    // routed call, ABSENT = the facade no_provider verdict that never rode the wire),
    // the return bytes, and the RESOLVED provider gid (engaged on a routed call, absent
    // on the no_provider leg). The Policy-free shape lifted into endpoint_seam.h.
    using on_reply_fn = io::on_reply_fn;

    template <typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn)
        : m_seam(n.endpoint_seam_for())
        , m_fqn(fqn)
    {
    }

    // The hot verb: resolve the first connected provider and dispatch the call; on no
    // provider, POST the no_provider completion (never inline). The completion is
    // adapted from the forwarder's (rpc_status, bytes) into expected<reply, error_code>:
    // a success status yields reply{bytes, info}; every failure maps via from_rpc_status.
    template <typename Completion>
    void call(std::span<const std::byte> param, Completion &&completion)
    {
        dispatch(param, std::forward<Completion>(completion), std::nullopt);
    }

    template <typename Completion>
    void call(std::span<const std::byte> param, const call_options &opts, Completion &&completion)
    {
        dispatch(param, std::forward<Completion>(completion), opts.deadline);
    }

    caller(caller &&) noexcept = default;
    caller &operator=(caller &&) noexcept = default;

    caller(const caller &) = delete;
    caller &operator=(const caller &) = delete;

    ~caller() = default;

private:
    template <typename Completion>
    void dispatch(std::span<const std::byte> param, Completion &&completion,
                  std::optional<std::chrono::nanoseconds> deadline)
    {
        m_seam.call(m_seam.ctx, m_fqn, param,
               [completion = std::forward<Completion>(completion)](
                   std::optional<wire::rpc_status> status, std::span<const std::byte> bytes,
                   const std::optional<publisher_gid> &provider) mutable {
                   if(!status)
                   {
                       completion(expected<reply, std::error_code>{
                           unexpect, make_error_code(call_errc::no_provider)});
                       return;
                   }
                   if(*status == wire::rpc_status::success)
                   {
                       reply r{bytes, reply_info{provider, 0, wire::now_timestamp_ns(), false}};
                       completion(expected<reply, std::error_code>{std::move(r)});
                       return;
                   }
                   completion(expected<reply, std::error_code>{
                       unexpect, make_error_code(from_rpc_status(*status))});
               },
               deadline);
    }

    io::endpoint_seam m_seam{};
    std::string       m_fqn;
};

// The typed calling endpoint: an encode/decode adaptation around the bytes caller.
// call(const Req&, completion[, options]) encodes the request via CReq and completes the
// caller with void(expected<Res, std::error_code>). There is NO inproc fast path for RPC
// by design — a request/response always rides bytes.
//
// The completion adapter:
//   - an ABSENT wire status is the facade no_provider verdict (call_errc::no_provider);
//   - rpc_status::success decodes Res via CRes (a decode failure → deserialize_failed);
//   - rpc_status::error with a well-formed varint error-leg payload reconstructs the
//     provider's error VALUE under provider_category (value preserved, category erased);
//   - an empty or MALFORMED error-leg payload falls back to from_rpc_status — so a bytes
//     procedure replying a bare error completes this typed caller with call_errc::error
//     (the interop fallback: a hostile varint never crashes, never half-decodes);
//   - every other failure leg passes through from_rpc_status unchanged.
template <typename Res, typename Req, typename CReq, typename CRes>
    requires typed_codec<CReq> && typed_codec<CRes>
class caller<Res(Req), CReq, CRes>
{
public:
    using on_reply_fn = io::on_reply_fn;

    template <typename Policy, typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn, CReq req_codec = {}, CRes res_codec = {})
        : m_seam(n.endpoint_seam_for())
        , m_req_codec(std::move(req_codec))
        , m_res_codec(std::move(res_codec))
        , m_fqn(fqn)
    {
    }

    template <typename Completion>
    void call(const Req &request, Completion &&completion)
    {
        dispatch(request, std::forward<Completion>(completion), std::nullopt);
    }

    template <typename Completion>
    void call(const Req &request, const call_options &opts, Completion &&completion)
    {
        dispatch(request, std::forward<Completion>(completion), opts.deadline);
    }

    caller(caller &&) noexcept = default;
    caller &operator=(caller &&) noexcept = default;

    caller(const caller &) = delete;
    caller &operator=(const caller &) = delete;

    ~caller() = default;

private:
    template <typename Completion>
    void dispatch(const Req &request, Completion &&completion,
                  std::optional<std::chrono::nanoseconds> deadline)
    {
        const wire_bytes<> encoded = m_req_codec.encode(request);
        m_seam.call(m_seam.ctx, m_fqn, static_cast<std::span<const std::byte>>(encoded),
               [completion = std::forward<Completion>(completion), res_codec = m_res_codec](
                   std::optional<wire::rpc_status> status, std::span<const std::byte> bytes,
                   const std::optional<publisher_gid> &) mutable {
                   if(!status)
                   {
                       completion(expected<Res, std::error_code>{
                           unexpect, make_error_code(call_errc::no_provider)});
                       return;
                   }
                   if(*status == wire::rpc_status::success)
                   {
                       Res value{};
                       if(res_codec.decode(bytes, value))
                           completion(expected<Res, std::error_code>{std::move(value)});
                       else
                           completion(expected<Res, std::error_code>{
                               unexpect, make_error_code(call_errc::deserialize_failed)});
                       return;
                   }
                   if(*status == wire::rpc_status::error)
                   {
                       std::size_t consumed = 0;
                       if(const auto provider_value = wire::read_varint(bytes, consumed);
                          provider_value && consumed == bytes.size())
                       {
                           completion(expected<Res, std::error_code>{
                               unexpect,
                               std::error_code{static_cast<int>(*provider_value), provider_category()}});
                           return;
                       }
                   }
                   completion(expected<Res, std::error_code>{
                       unexpect, make_error_code(from_rpc_status(*status))});
               },
               deadline);
    }

    io::endpoint_seam m_seam{};
    CReq        m_req_codec;
    CRes        m_res_codec;
    std::string m_fqn;
};

// The codec-family spelling: one class-template Family expands to the per-half codecs
// Family<Req> / Family<Res> over a Res(Req) signature, collapsing the verbose four-argument
// form to rpc<div_response(div_request), pair_codec>. An alias rather than a class because a
// single class-template name cannot carry both a template-template Family in position two AND
// the per-half typename CReq/CRes escape — the kinds differ and a partial specialization
// cannot change a parameter's kind. The per-half class caller<Sig, CReq, CRes> remains the
// general form and the asymmetric-serializer escape; Family<half> must satisfy typed_codec
// (the typed specialization's requires-clause enforces it). Family is NON-defaulted (the user
// always spells it; this also sidesteps the MSVC template-template defaulting hazard).
template <typename Sig, template <typename> class Family>
using rpc_caller =
    caller<Sig, Family<detail::request_of_t<Sig>>, Family<detail::response_of_t<Sig>>>;

}

#endif
