#ifndef HPP_GUARD_PLEXUS_IO_SHM_NOTIFIER_CONCEPT_H
#define HPP_GUARD_PLEXUS_IO_SHM_NOTIFIER_CONCEPT_H

#include "plexus/detail/compat.h"

#include <concepts>

namespace plexus::io::shm {

// The notifier seam: the three-op cross-process wakeup interface the ring channel
// borrows BY REFERENCE, defined in core so no core translation unit ever pulls a
// kernel futex header or an asio header. The wakeup MECHANISM is the backend's —
// the compiled plexus-shm futex primitive and the plexus-asio reactor bridge each
// SATISFY this same seam; only the wakeup primitive swaps (io_uring futex-wait
// thread-free primary, the bounded-thread+eventfd bridge as the portable floor).
//
//   signal()       the producer wake: bump the shared generation word and wake any
//                  waiter. Called on the publish path after a slot commits.
//   arm(drain)     register the consumer's drain callback with the reactor and begin
//                  watching the word; on each cross-process wake the bridge posts the
//                  drain onto the user's executor (no plexus-owned loop). The drain is
//                  a move_only_function<void()> (the project move-only-callback
//                  convention — NOT the copyable std-function wrapper), so the seam
//                  never forces a copyable callable.
//   disarm()       stop watching and release the reactor registration. The teardown
//                  ordering (disarm BEFORE the subscribers the drain touches are
//                  destroyed) is the non-negotiable rule the registry enforces.
//
// The verbs are bare-call-expression typed (the byte_channel / transport_backend
// idiom): the void verbs are constrained, arm() takes the drain by move.
template<typename T>
concept notifier = requires(T &n, plexus::detail::move_only_function<void()> drain) {
    { n.signal() } -> std::same_as<void>;
    n.arm(std::move(drain));
    { n.disarm() } -> std::same_as<void>;
};

}

#endif
