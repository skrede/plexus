#ifndef HPP_GUARD_PLEXUS_IO_ENDPOINT_SEAM_H
#define HPP_GUARD_PLEXUS_IO_ENDPOINT_SEAM_H

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/object_carrier.h"

#include "plexus/topic_qos.h"
#include "plexus/publisher_gid.h"

#include "plexus/wire/rpc_status.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io {

// The alloc-free encode carrier that crosses the endpoint seam. It mirrors the
// loan_slot release-fn-ptr idiom (object_carrier.h): a borrowed state pointer plus a
// captureless trampoline, so an erased encode is a two-word POD that allocates nothing
// regardless of how much the source lambda captures. state is bound per-publish to a
// lambda owned by the calling stack frame; the thunk's lifetime is that synchronous
// verb (the same capture lifetime publisher.h's lazy lambda already relies on).
struct encode_thunk
{
    void *state;
    std::span<const std::byte> (*invoke)(void *state);
};

// Bind a stack lambda by reference into an encode_thunk. The trampoline is captureless
// (a function, not a closure) so it lowers to a plain function pointer; the lambda's own
// captures are reached through state, never copied into the thunk.
template<typename Fn>
[[nodiscard]] encode_thunk make_encode_thunk(Fn &fn) noexcept
{
    return encode_thunk{&fn, [](void *state) -> std::span<const std::byte>
                        { return (*static_cast<Fn *>(state))(); }};
}

inline std::span<const std::byte> invoke(const encode_thunk &thunk)
{
    return thunk.invoke(thunk.state);
}

// The Policy-free callable shapes the endpoint handles and the node exchange across the
// seam. They are structurally identical across every policy (the req/res signatures name
// only rpc_status + spans), so the seam names them once here
// instead of through a Policy-parameterized forwarder type. Spelled FULLY QUALIFIED as
// plexus::detail::move_only_function — io::detail shadows plexus::detail in this scope.
using reply_fn =
        plexus::detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;
using handler_fn =
        plexus::detail::move_only_function<void(std::span<const std::byte> param, reply_fn &)>;
using on_reply_fn = plexus::detail::move_only_function<void(std::optional<wire::rpc_status>,
                                                            std::span<const std::byte>,
                                                            const std::optional<publisher_gid> &)>;
using bytes_cb =
        plexus::detail::move_only_function<void(std::span<const std::byte>, const message_info &)>;
using object_dispatch =
        plexus::detail::move_only_function<void(const object_carrier &, const message_info &)>;

// The object-lane dispatch entry a typed subscriber registers alongside its bytes
// adapter (under the one registration id). native_key is the process-local C++ type
// witness (the address of a per-T inline constant); dispatch recovers the concrete T from
// the carrier's slot. A NULL native_key marks a bytes-only subscription with no entry.
struct object_entry
{
    const void     *native_key{};
    object_dispatch dispatch;
};

// The type-erased outbound-verb seam: a POD of one function pointer per outbound endpoint
// verb plus the node ctx they recover. Filled by the node at endpoint construction
// (endpoint_seam_for); each fn-ptr is a captureless static trampoline forwarding to a
// private node member. The handles cross exactly one indirect call per outbound verb; the
// inbound delivery path never touches this seam. ctx is a detail-only void* recovered
// solely inside the trampolines — it is never a public handle signature.
struct endpoint_seam
{
    void *ctx;

    // geometry is an OPAQUE per-topic same-host provisioning override the node recovers (null =
    // none): the seam stays transport-name-free, mirroring ctx/encode_thunk's void*-erasure. It
    // points at a caller-stack value alive for this synchronous declare verb only.
    void (*declare_publisher)(void *ctx, std::string_view fqn, const topic_qos &qos,
                              bool emit_source_identity, std::optional<std::uint64_t> type_id,
                              const void *geometry, std::optional<topic_capture_rule> capture);
    void (*publish)(void *ctx, std::string_view fqn, std::span<const std::byte> bytes);
    void (*publish_object)(void *ctx, std::string_view fqn, const object_carrier &carrier,
                           encode_thunk encode);

    std::uint64_t (*register_subscriber)(void *ctx, std::string_view fqn, const subscriber_qos &qos,
                                         bytes_cb cb, std::optional<std::uint64_t> type_id,
                                         const void *native_key, object_dispatch dispatch,
                                         std::optional<topic_capture_rule> capture);
    void (*retire_subscriber)(void *ctx, std::uint64_t rid);
    void (*retire_publisher)(void *ctx, std::string_view fqn);

    void (*serve_procedure)(void *ctx, std::string_view fqn, handler_fn handler);
    void (*retire_procedure)(void *ctx, std::string_view fqn);

    void (*call)(void *ctx, std::string_view fqn, std::span<const std::byte> param,
                 on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline);
};

}

#endif
