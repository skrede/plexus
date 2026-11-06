#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/run_task.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <chrono>
#include <cstdint>
#include <utility>

namespace plexus::freertos {

// The receive timeout bounds every blocking receive and slot wait of the task, so a task asked to exit sees the request
// within one timeout. The conversion from task_options keeps the timeout's default for callers that name only the task.
struct rx_task_options
{
    rx_task_options(task_options task, std::chrono::milliseconds recv_timeout = std::chrono::milliseconds{100})
            : task(task)
            , recv_timeout(recv_timeout)
    {
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

// One turn: on an exit request close the socket, post exited and return false; otherwise acquire a slot, recv into it
// and post the feed, parking on a full queue until the executor drains room, so on a stream the TCP window closes while
// the task waits and no byte is lost. A recv of 0 (the receive timeout, or EWOULDBLOCK) releases the slot.
template<plexus::stream::stream_socket S>
bool lwip_rx_step(lwip_rx_ctx<S> &ctx)
{
    if(ctx.channel.rx().exit_requested())
    {
        ctx.channel.rx().close_and_post_exited();
        return false;
    }
    auto *slot = ctx.channel.rx().acquire(ctx.wait);
    if(!slot)
        return true;
    if(ctx.channel.recv_into_slot(*slot) > 0)
        post_feed(ctx, *slot);
    else
        ctx.channel.rx().release(*slot);
    return true;
}

template<plexus::stream::stream_socket S>
void lwip_rx_trampoline(void *arg)
{
    auto *ctx = static_cast<lwip_rx_ctx<S> *>(arg);
    while(lwip_rx_step(*ctx))
    {
    }
    // FreeRTOS aborts a task that returns, so the task frees its context and deletes itself once it has posted exited.
    delete ctx;
    vTaskDelete(nullptr);
}

}

// The task is marked live before it is created, so a close that races its first turn already asks it to exit; the
// heap-allocated context is owned by the spawned task, and freed here only when the task could not be created.
template<plexus::stream::stream_socket S>
BaseType_t spawn_lwip_rx_task(lwip_channel<S> &channel, freertos_executor &ex, rx_task_options opts)
{
    auto *ctx = new detail::lwip_rx_ctx<S>{channel, ex, pdMS_TO_TICKS(static_cast<std::uint32_t>(opts.recv_timeout.count()))};
    channel.rx().started();
    const BaseType_t created = xTaskCreate(&detail::lwip_rx_trampoline<S>, "plexus-rx", opts.task.stack, ctx, opts.task.priority, nullptr);
    if(created != pdTRUE)
    {
        delete ctx;
        channel.rx().start_failed();
    }
    return created;
}

}

#endif
