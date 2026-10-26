#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_RX_TASK_H

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/run_task.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <utility>

namespace plexus::freertos {

namespace detail {

// The spawned RX task owns this context for the channel's lifetime (the loop runs until the channel
// closes), so the borrowed channel and executor it points at must outlive it — the caller's
// contract, mirroring run_task_ctx. The RX task's sole verbs are recv + post_from_task; it touches
// no engine state, so the engine stays single-threaded on the executor task.
template<plexus::stream::stream_socket S>
struct lwip_rx_ctx
{
    lwip_channel<S>   &channel;
    freertos_executor &executor;
};

// portMAX_DELAY waits indefinitely only with INCLUDE_vTaskSuspend; where it can elapse the post is
// retried while the channel is open, and once the channel has closed the slot goes back to the pool.
template<plexus::stream::stream_socket S>
void post_feed(lwip_rx_ctx<S> &ctx, typename lwip_channel<S>::rx_slot &slot)
{
    const posted_work feed{&lwip_channel<S>::invoke_feed, &slot};
    while(!ctx.executor.post_from_task(feed, portMAX_DELAY))
    {
        if(ctx.channel.closed())
        {
            ctx.channel.release_rx_slot(slot);
            return;
        }
    }
}

// Acquire a free pool slot (parking until one frees), recv into it, and post the feed across to the
// executor task, parking on a full queue until the executor drains room: on a stream the TCP window
// closes while the task waits, so no byte is lost. A recv of 0 with the socket still open is
// EWOULDBLOCK on a non-blocking host build (the on-target socket is blocking, so the task parks in
// recv) and releases the slot. Returns false once the channel has closed so the loop exits and the
// engine re-dials on its own reconnect timer (the RX task never re-dials).
template<plexus::stream::stream_socket S>
bool lwip_rx_step(lwip_rx_ctx<S> &ctx)
{
    auto *slot = ctx.channel.acquire_rx_slot(portMAX_DELAY);
    if(!slot)
        return true;
    if(ctx.channel.recv_into_slot(*slot) > 0)
        post_feed(ctx, *slot);
    else
        ctx.channel.release_rx_slot(*slot);
    return !ctx.channel.closed();
}

template<plexus::stream::stream_socket S>
void lwip_rx_trampoline(void *arg)
{
    auto *ctx = static_cast<lwip_rx_ctx<S> *>(arg);
    while(lwip_rx_step(*ctx))
    {
    }
    // The loop exits when the channel closes; FreeRTOS aborts a task that returns, so free the owned
    // context and delete the task rather than falling off the end (the engine re-dials on its own timer).
    delete ctx;
    vTaskDelete(nullptr);
}

}

// Spawn the dedicated transport RX task for one channel with an EXPLICIT stack (no default): a
// blocking recv into a pooled slot + a zero-alloc post per turn. The borrowed channel and executor
// must outlive the task (run-never-returns); the heap-allocated context is owned by the spawned task.
template<plexus::stream::stream_socket S>
BaseType_t spawn_lwip_rx_task(lwip_channel<S> &channel, freertos_executor &ex, task_options topts)
{
    auto *ctx = new detail::lwip_rx_ctx<S>{channel, ex};
    return xTaskCreate(&detail::lwip_rx_trampoline<S>, "plexus-rx", topts.stack, ctx, topts.priority, nullptr);
}

}

#endif
