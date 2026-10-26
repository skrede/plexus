#ifndef HPP_GUARD_PLEXUS_FREERTOS_POSTED_WORK_H
#define HPP_GUARD_PLEXUS_FREERTOS_POSTED_WORK_H

namespace plexus::freertos {

// A unit of cross-context work carried by value through the executor queue, a trivially
// copyable POD. The producer owns the storage `ctx` points at; `invoke(ctx)` runs the work on
// the executor task AND releases that storage back to the producer's pool, so the executor never
// owns or frees the bytes and the post path allocates nothing.
struct posted_work
{
    void (*invoke)(void *) noexcept;
    void *ctx;
};

}

#endif
