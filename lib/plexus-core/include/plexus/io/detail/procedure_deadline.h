#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DEADLINE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DEADLINE_H

#include "plexus/wire/rpc_status.h"

#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::io::detail {

// A no-op if the entry already resolved, so the expiry is idempotent against a cancel race.
template<typename Table>
void fire_timeout(Table &table, const std::string &node_name, std::uint64_t corr_id)
{
    auto peer_it = table.find(node_name);
    if(peer_it == table.end())
        return;
    auto entry_it = peer_it->second.find(corr_id);
    if(entry_it == peer_it->second.end())
        return;
    auto pending = std::move(entry_it->second);
    peer_it->second.erase(entry_it);
    pending.on_response(wire::rpc_status::timeout, {});
}

// The handler looks the entry up by (node_name, corr_id) in the table it was inserted into rather than capturing it,
// which moves. Each wait is one operation of the forwarder, which outlives the entry's timer and owns the table.
template<typename Forwarder, typename Table>
void arm_deadline(Forwarder &f, Table &table, const std::string &node_name, std::uint64_t corr_id, typename Forwarder::timer_type &timer, std::chrono::nanoseconds deadline)
{
    timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(deadline));
    f.pending().begin();
    timer.async_wait(
            [&f, &table, node_name, corr_id](std::error_code ec)
            {
                f.pending().end();
                if(!ec)
                    detail::fire_timeout(table, node_name, corr_id);
            });
}

// Moved out before any callback fires: a completion may issue a call that inserts into the table, rehashing it or
// mutating this peer's entries under a live iterator.
template<typename Table>
typename Table::mapped_type take_peer_calls(Table &table, const std::string &node_name)
{
    auto peer_it = table.find(node_name);
    if(peer_it == table.end())
        return {};
    auto taken = std::move(peer_it->second);
    table.erase(peer_it);
    return taken;
}

template<typename Calls>
void fail_calls(Calls &calls, wire::rpc_status status)
{
    for(auto &[corr_id, pending] : calls)
    {
        pending.timer->cancel();
        pending.on_response(status, {});
    }
}

template<typename Table, typename Visit>
void for_each_call(Table &table, Visit &&visit)
{
    for(auto &[node_name, per_peer] : table)
        for(auto &[corr_id, pending] : per_peer)
            visit(pending);
}

template<typename Table>
std::size_t count_calls(const Table &table) noexcept
{
    std::size_t n = 0;
    for(const auto &[node_name, per_peer] : table)
        n += per_peer.size();
    return n;
}

// The count closes after every timer was canceled and before any call completes, so a call a completion issues
// completes at once with canceled instead of arming a wait.
template<typename Forwarder>
void close_forwarder(Forwarder &f)
{
    if(f.m_ops.closing())
        return;
    auto direct       = std::exchange(f.m_outstanding, {});
    auto forwarded    = std::exchange(f.m_forwarded_outstanding, {});
    const auto cancel = [](auto &pending) { pending.timer->cancel(); };
    detail::for_each_call(direct, cancel);
    detail::for_each_call(forwarded, cancel);
    f.m_ops.close();
    const auto complete = [](auto &pending) { pending.on_response(wire::rpc_status::canceled, {}); };
    detail::for_each_call(direct, complete);
    detail::for_each_call(forwarded, complete);
}

}

#endif
