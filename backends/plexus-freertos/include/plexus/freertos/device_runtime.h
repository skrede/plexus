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
// quiescence, then yield the core for a watchdog-safe interval. The single-ref and
// variadic forms below build a local pollable*[] and forward here, so there is one
// loop and no per-type-combination expansion.
template<pollable P>
void tick(freertos_executor &ex, std::span<P *const> ps, run_options opts)
{
    for(P *p : ps)
        p->poll();
    ex.drain();
    vTaskDelay(pdMS_TO_TICKS(static_cast<std::uint32_t>(effective_park(opts).count())));
}

template<pollable P>
void tick(freertos_executor &ex, P &p, run_options opts = {})
{
    P *one[] = {&p};
    tick(ex, std::span<P *const>{one}, opts);
}

template<pollable P, pollable... Rest>
    requires(sizeof...(Rest) >= 1)
void tick(freertos_executor &ex, P &p, Rest &...rest)
{
    P *all[] = {&p, &rest...};
    tick(ex, std::span<P *const>{all}, run_options{});
}

template<pollable P>
[[noreturn]] void run(freertos_executor &ex, std::span<P *const> ps, run_options opts)
{
    for(;;)
        tick(ex, ps, opts);
}

template<pollable P>
[[noreturn]] void run(freertos_executor &ex, P &p, run_options opts = {})
{
    P *one[] = {&p};
    run(ex, std::span<P *const>{one}, opts);
}

template<pollable P, pollable... Rest>
    requires(sizeof...(Rest) >= 1)
[[noreturn]] void run(freertos_executor &ex, P &p, Rest &...rest)
{
    P *all[] = {&p, &rest...};
    run(ex, std::span<P *const>{all}, run_options{});
}

}

#endif
