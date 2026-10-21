#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DEADLINE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DEADLINE_H

#include "plexus/wire/rpc_status.h"

#include <chrono>
#include <string>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::io::detail {

// A no-op if the entry already resolved, so the expiry is idempotent against a cancel race.
template<typename Forwarder>
void fire_timeout(Forwarder &f, const std::string &node_name, std::uint64_t corr_id)
{
    auto peer_it = f.m_outstanding.find(node_name);
    if(peer_it == f.m_outstanding.end())
        return;
    auto entry_it = peer_it->second.find(corr_id);
    if(entry_it == peer_it->second.end())
        return;
    auto pending = std::move(entry_it->second);
    peer_it->second.erase(entry_it);
    pending.on_response(wire::rpc_status::timeout, {});
}

// The handler looks the entry up by (node_name, corr_id) rather than capturing it, which moves. Each wait is one
// operation of the forwarder, which outlives the entry's timer.
template<typename Forwarder>
void arm_deadline(Forwarder &f, const std::string &node_name, std::uint64_t corr_id, typename Forwarder::timer_type &timer, std::chrono::nanoseconds deadline)
{
    timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(deadline));
    f.pending().begin();
    timer.async_wait(
            [&f, node_name, corr_id](std::error_code ec)
            {
                f.pending().end();
                if(!ec)
                    detail::fire_timeout(f, node_name, corr_id);
            });
}

// The count closes after every timer was canceled and before any call completes, so a call a completion issues
// completes at once with canceled instead of arming a wait.
template<typename Forwarder>
void close_forwarder(Forwarder &f)
{
    if(f.m_ops.closing())
        return;
    auto detached = std::exchange(f.m_outstanding, {});
    for(auto &[node_name, per_peer] : detached)
        for(auto &[corr_id, pending] : per_peer)
            pending.timer->cancel();
    f.m_ops.close();
    for(auto &[node_name, per_peer] : detached)
        for(auto &[corr_id, pending] : per_peer)
            pending.on_response(wire::rpc_status::canceled, {});
}

}

#endif
