#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/run_task.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/detail/fail_closed.h"

#include <chrono>
#include <memory>
#include <cstdint>
#include <algorithm>

namespace plexus::freertos {

// The receive timeout bounds each blocking receive and each slot wait of the task. The two stack, so a task asked to exit
// sees the request within two receive timeouts while the executor keeps pumping. The conversion from task_options keeps
// the timeout's default for callers that name only the task.
struct rx_task_options
{
    rx_task_options(task_options task, std::chrono::milliseconds recv_timeout = std::chrono::milliseconds{100})
            : task(task)
            , recv_timeout(recv_timeout)
    {
        if(recv_timeout <= std::chrono::milliseconds::zero())
            plexus::detail::fail_closed("rx_task_options: the receive timeout must be positive");
    }

    task_options task;
    std::chrono::milliseconds recv_timeout;
};

namespace detail {

// The spawned RX task owns this context until it exits on the channel's exit request, so the borrowed channel and
// executor it points at must outlive the task. The RX task's verbs are recv, post_from_task and, on exit, the socket's
// close; it touches no engine state, so the engine stays single-threaded on the executor task.
template<plexus::stream::stream_socket S>
struct lwip_rx_ctx
{
    lwip_channel<S> &channel;
    freertos_executor &executor;
    TickType_t wait;
};

// portMAX_DELAY waits indefinitely only with INCLUDE_vTaskSuspend; where it can elapse the post is
// retried until the task is asked to exit, and then the slot goes back to the pool.
template<plexus::stream::stream_socket S>
void post_feed(lwip_rx_ctx<S> &ctx, typename lwip_channel<S>::rx_slot &slot)
{
    const posted_work feed{&lwip_channel<S>::rx_type::invoke_feed, &slot};
    while(!ctx.executor.post_from_task(feed, portMAX_DELAY))
    {
        if(ctx.channel.rx().exit_requested())
        {
            ctx.channel.rx().release(slot);
            return;
        }
    }
}

// A full executor queue parks the feed post, so on a stream the TCP window closes while the task waits and no byte is
// lost.
template<plexus::stream::stream_socket S>
bool lwip_rx_step(lwip_rx_ctx<S> &ctx)
{
    auto &rx = ctx.channel.rx();
    if(rx.exit_requested())
    {
        rx.close_and_post_exited();
        return false;
    }
    auto *slot = rx.acquire(ctx.wait);
    if(!slot)
        return true;
    if(rx.recv_into(*slot) > 0)
    {
        post_feed(ctx, *slot);
        return true;
    }
    rx.release(*slot);
    if(!rx.socket_dropped())
        return true;
    rx.post_drop_observed();
    rx.post_exited();
    return false;
}

template<plexus::stream::stream_socket S>
void lwip_rx_trampoline(void *arg)
{
    // The spawned task owns its context. FreeRTOS aborts a task that returns, so the task deletes itself, and the
    // owner's scope closes first because vTaskDelete(nullptr) does not return on target.
    {
        const std::unique_ptr<lwip_rx_ctx<S>> ctx{static_cast<lwip_rx_ctx<S> *>(arg)};
        while(lwip_rx_step(*ctx))
        {
        }
    }
    vTaskDelete(nullptr);
}

}

// The task is marked live before it is created, so a close that races its first turn already asks it to exit. The
// context passes to the spawned task, and comes back to this owner only when the task could not be created.
template<plexus::stream::stream_socket S>
BaseType_t spawn_lwip_rx_task(lwip_channel<S> &channel, freertos_executor &ex, rx_task_options opts)
{
    // A slot wait of 0 never parks, so a timeout under one tick would spin the task on an exhausted pool.
    const TickType_t wait = std::max<TickType_t>(1, pdMS_TO_TICKS(static_cast<std::uint32_t>(opts.recv_timeout.count())));
    auto ctx              = std::make_unique<detail::lwip_rx_ctx<S>>(channel, ex, wait);
    channel.rx().started();
    auto *const arg          = ctx.release();
    const BaseType_t created = xTaskCreate(&detail::lwip_rx_trampoline<S>, "plexus-rx", opts.task.stack, arg, opts.task.priority, nullptr);
    if(created != pdTRUE)
    {
        ctx.reset(arg);
        channel.rx().start_failed();
    }
    return created;
}

}

#endif
