#ifndef HPP_GUARD_PLEXUS_IO_ENDPOINT_SEAM_H
#define HPP_GUARD_PLEXUS_IO_ENDPOINT_SEAM_H

#include <span>
#include <cstddef>

namespace plexus::io {

// The alloc-free encode carrier that crosses the endpoint seam. It mirrors the
// loan_slot release-fn-ptr idiom (object_carrier.h): a borrowed state pointer plus a
// captureless trampoline, so an erased encode is a two-word POD that allocates nothing
// regardless of how much the source lambda captures. state is bound per-publish to a
// lambda owned by the calling stack frame; the thunk's lifetime is that synchronous
// verb (the same capture lifetime publisher.h's lazy lambda already relies on).
struct encode_thunk
{
    void                                      *state;
    std::span<const std::byte>               (*invoke)(void *state);
};

// Bind a stack lambda by reference into an encode_thunk. The trampoline is captureless
// (a function, not a closure) so it lowers to a plain function pointer; the lambda's own
// captures are reached through state, never copied into the thunk.
template <typename Fn>
[[nodiscard]] encode_thunk make_encode_thunk(Fn &fn) noexcept
{
    return encode_thunk{
        &fn,
        [](void *state) -> std::span<const std::byte> {
            return (*static_cast<Fn *>(state))();
        }};
}

inline std::span<const std::byte> invoke(const encode_thunk &thunk)
{
    return thunk.invoke(thunk.state);
}

}

#endif
