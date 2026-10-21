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

// The handler looks the entry up by (node_name, corr_id) rather than capturing it, which moves.
template<typename Forwarder>
void arm_deadline(Forwarder &f, const std::string &node_name, std::uint64_t corr_id, typename Forwarder::timer_type &timer, std::chrono::nanoseconds deadline)
{
    timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(deadline));
    timer.async_wait(
            [&f, node_name, corr_id](std::error_code ec)
            {
                if(ec)
                    return;
                detail::fire_timeout(f, node_name, corr_id);
            });
}

}

#endif
