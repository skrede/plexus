#ifndef HPP_GUARD_PLEXUS_FREERTOS_RUN_TASK_H
#define HPP_GUARD_PLEXUS_FREERTOS_RUN_TASK_H

#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <array>
#include <cstdint>
#include <algorithm>

namespace plexus::freertos {

struct task_options
{
    task_options(std::uint32_t stack, UBaseType_t priority = 5)
            : stack(stack)
            , priority(priority)
    {
    }

    std::uint32_t stack;
    UBaseType_t   priority;
};

namespace detail {

// The spawned task owns this context for the device's lifetime (run never returns),
// so the borrowed substrate it points at must outlive the task — the caller's
// contract. It copies the poll-handle pointers (not the pollables) so a caller-stack
// array used to build the list need not survive past run_task.
template<pollable P, std::size_t N>
struct run_task_ctx
{
    freertos_executor &ex;
    std::array<P *, N> ps;
    run_options        opts;
};

template<pollable P, std::size_t N>
void run_task_trampoline(void *arg)
{
    auto &ctx = *static_cast<run_task_ctx<P, N> *>(arg);
    run(ctx.ex, std::span<P *const>{ctx.ps}, ctx.opts);
}

template<pollable P, std::size_t N>
BaseType_t spawn_run_task(freertos_executor &ex, const std::array<P *, N> &ps, task_options topts, run_options opts)
{
    auto *ctx = new run_task_ctx<P, N>{ex, ps, opts};
    return xTaskCreate(&run_task_trampoline<P, N>, "plexus", topts.stack, ctx, topts.priority, nullptr);
}

}

template<pollable P, std::size_t N>
BaseType_t run_task(freertos_executor &ex, std::span<P *const, N> ps, task_options topts, run_options opts = {})
{
    std::array<P *, N> copy{};
    std::copy(ps.begin(), ps.end(), copy.begin());
    return detail::spawn_run_task(ex, copy, topts, opts);
}

template<pollable P>
BaseType_t run_task(freertos_executor &ex, P &p, task_options topts, run_options opts = {})
{
    return detail::spawn_run_task(ex, std::array<P *, 1>{&p}, topts, opts);
}

template<pollable P, pollable... Rest>
    requires(sizeof...(Rest) >= 1)
BaseType_t run_task(freertos_executor &ex, task_options topts, P &p, Rest &...rest)
{
    return detail::spawn_run_task(ex, std::array<P *, 1 + sizeof...(Rest)>{&p, &rest...}, topts, run_options{});
}

}

#endif
