#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCESS_LOOPBACK_POST_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCESS_LOOPBACK_POST_H

#include "plexus/io/detail/pending_ops.h"

#include "plexus/io/object_carrier.h"

#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// The object fast-path post: addref BEFORE the executor hop so the producing loan slot survives
// until the posted dispatch runs, release AFTER the hook returns. Mirrors the inproc send_object /
// deliver_object refcount transfer (the bus's reference is owned by the delivered callback).
template<typename Policy, typename OnObject>
void post_object(typename Policy::executor_type executor, pending_ops &ops, OnObject &on_object_cb, const object_carrier &carrier)
{
    addref(carrier);
    ops.begin();
    Policy::post(executor,
                 [&ops, &on_object_cb, carrier]() mutable
                 {
                     ops.end();
                     if(on_object_cb)
                         on_object_cb(carrier);
                     release(carrier);
                 });
}

// The bytes lane post: the framed buffer is copied into an owned vector the closure carries, so the
// publisher's transient frame need not outlive the synchronous send() — delivery fires on a later
// executor turn, never inside send().
template<typename Policy, typename OnData>
void post_bytes(typename Policy::executor_type executor, pending_ops &ops, OnData &on_data_cb, std::span<const std::byte> bytes)
{
    ops.begin();
    Policy::post(executor,
                 [&ops, &on_data_cb, owned = std::vector<std::byte>(bytes.begin(), bytes.end())]()
                 {
                     ops.end();
                     if(on_data_cb)
                         on_data_cb(std::span<const std::byte>{owned});
                 });
}

}

#endif
