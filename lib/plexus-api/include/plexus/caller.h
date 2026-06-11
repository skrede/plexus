#ifndef HPP_GUARD_PLEXUS_CALLER_H
#define HPP_GUARD_PLEXUS_CALLER_H

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/call_error.h"

#include "plexus/io/procedure_forwarder.h"

#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/publisher_gid.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

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

// A move-only RAII calling endpoint: the CONSTRUCTOR binds the node and fqn; the handle
// owns the call verb. The caller is CALLBACK-ONLY — there is no blocking/future form (a
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
template <typename Policy>
    requires plexus::Policy<Policy>
class caller
{
public:
    // The completion-side callback the node seam fans: the wire status (ENGAGED on a
    // routed call, ABSENT = the facade no_provider verdict that never rode the wire),
    // the return bytes, and the RESOLVED provider gid (engaged on a routed call, absent
    // on the no_provider leg).
    using on_reply_fn = plexus::detail::move_only_function<void(
        std::optional<wire::rpc_status>, std::span<const std::byte>,
        const std::optional<publisher_gid> &)>;

    template <typename... NodeTs>
    caller(node<Policy, NodeTs...> &n, std::string_view fqn)
        : m_call([&n](std::string_view f, std::span<const std::byte> p,
                      on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline)
                 { n.call_seam(f, p, std::move(on_reply), deadline); })
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
        m_call(m_fqn, param,
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

    plexus::detail::move_only_function<void(
        std::string_view, std::span<const std::byte>, on_reply_fn,
        std::optional<std::chrono::nanoseconds>)> m_call;
    std::string m_fqn;
};

}

#endif
