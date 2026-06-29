#ifndef HPP_GUARD_PLEXUS_FREERTOS_DEVICE_RUNTIME_H
#define HPP_GUARD_PLEXUS_FREERTOS_DEVICE_RUNTIME_H

#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <span>
#include <chrono>
#include <algorithm>

namespace plexus::freertos {

template<class P>
concept pollable = requires(P p) {
    { p.poll() };
};

// The watchdog-safe park floor: one tick at the ESP-IDF default 1000 Hz rate. A
// park clamped to this can never collapse into the busy-spin that starves the idle
// task and trips the task watchdog, the one footgun this facade exists to remove.
inline constexpr std::chrono::milliseconds k_min_park{1};

struct run_options
{
    run_options(std::chrono::milliseconds park = std::chrono::milliseconds{10})
            : park(park)
    {
    }

    std::chrono::milliseconds park;
};

std::chrono::milliseconds effective_park(run_options opts) noexcept
{
    return std::max(opts.park, k_min_park);
}

// The one loop body: drain every poll handle into ready work, run the executor to
// quiescence, then yield the core for a watchdog-safe interval. run_task spawns a task
// that owns a homogeneous handle array, so this span form is retained for it.
template<pollable P>
void tick(freertos_executor &ex, std::span<P *const> ps, run_options opts)
{
    for(P *p : ps)
        p->poll();
    ex.drain();
    ex.park(effective_park(opts));
}

// The fold is the heterogeneous analog of the span loop: each poll() is a direct,
// statically-dispatched call, so the pollables need not share a type.
template<pollable... Ps>
void tick(freertos_executor &ex, run_options opts, Ps &...ps)
{
    (ps.poll(), ...);
    ex.drain();
    ex.park(effective_park(opts));
}

template<pollable... Ps>
    requires(sizeof...(Ps) >= 1)
void tick(freertos_executor &ex, Ps &...ps)
{
    tick(ex, run_options{}, ps...);
}

template<pollable P>
[[noreturn]] void run(freertos_executor &ex, std::span<P *const> ps, run_options opts)
{
    for(;;)
        tick(ex, ps, opts);
}

template<pollable... Ps>
[[noreturn]] void run(freertos_executor &ex, run_options opts, Ps &...ps)
{
    for(;;)
        tick(ex, opts, ps...);
}

template<pollable... Ps>
    requires(sizeof...(Ps) >= 1)
[[noreturn]] void run(freertos_executor &ex, Ps &...ps)
{
    run(ex, run_options{}, ps...);
}

}

#endif
