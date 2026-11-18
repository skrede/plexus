#ifndef HPP_GUARD_TESTS_INTEGRATION_PEER_SESSION_INPROC_LIFECYCLE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_PEER_SESSION_INPROC_LIFECYCLE_COMMON_H

#include "test_peer_session_inproc_common.h"

#include "plexus/executor_verbs.h"

namespace peer_session_inproc_fixture {

struct manual_clock
{
    using duration                                   = std::chrono::nanoseconds;
    using rep                                        = duration::rep;
    using period                                     = duration::period;
    using time_point                                 = std::chrono::time_point<manual_clock>;
    [[maybe_unused]] static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

struct manual_policy : plexus::executor_verbs<inproc_executor<manual_clock>>
{
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<manual_policy>);

using manual_session = plexus::io::peer_session<manual_policy>;
using manual_msg     = plexus::io::message_forwarder<manual_policy>;
using manual_rpc     = plexus::io::procedure_forwarder<manual_policy>;

// A lone requester with no responder answering: the handshake never completes, so the armed
// handshake timer resolves it.
struct timeout_harness
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    inproc_channel<manual_clock> peer_ch{ex}; // a silent peer that never responds

    plexus::log::null_logger sink;
    manual_msg messages{sink};
    manual_rpc procedures{ex, std::chrono::hours(1), sink};

    plexus::io::peer_context<manual_policy> ctx; // the record owns the dialer channel
    manual_session requester;

    explicit timeout_harness(std::chrono::nanoseconds timeout)
            : ctx{std::make_unique<inproc_channel<manual_clock>>(ex), {}, "silent-node", {}, {}}
            , requester(ctx, ex, make_cfg(0x02), timeout, messages, procedures, false, sink)
    {
        ctx.channel->connect_to(peer_ch.local_endpoint()); // sends land on a peer that never replies
        requester.start();
    }

    void drive()
    {
        ex.drain();
    }
};

}

#endif
