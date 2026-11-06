#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DELIVER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_DELIVER_H

#include "plexus/io/detail/procedure_fanout.h"

#include "plexus/node_id.h"

#include "plexus/log/logger.h"

#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/rpc_status.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::io::detail {

template<typename Forwarder, typename Peer>
bool deliver_request(Forwarder &f, const Peer &p, std::span<const std::byte> inner, std::uint64_t session_id, const node_id *reply_origin)
{
    auto decoded = wire::decode_rpc_request(inner);
    if(!decoded)
        return false;

    const typename Forwarder::reply_context ctx{&p.channel, decoded->header, session_id, reply_origin != nullptr, reply_origin ? *reply_origin : node_id{}};

    auto hash_it = f.m_hash_to_fqn.find(ctx.req_hdr.topic_hash);
    if(hash_it == f.m_hash_to_fqn.end())
    {
        auto status = f.m_endpoint.registry().fqn_for(ctx.req_hdr.topic_hash).empty() ? wire::rpc_status::no_handler : wire::rpc_status::topic_not_found;
        f.emit_reply(ctx, status, {});
        return true;
    }
    f.bind_reply(ctx);
    detail::emit_rpc_serve(f, hash_it->second, ctx.req_hdr.correlation_id);
    f.m_providers.find(hash_it->second)->second(decoded->param_data, f.m_reply);
    return true;
}

inline bool drop_orphan_response(log::logger &logger)
{
    logger.warn("plexus: forwarder rpc_response_orphan");
    return true;
}

template<typename Forwarder, typename Table>
bool deliver_response(Forwarder &f, Table &table, const std::string &node_name, std::span<const std::byte> inner)
{
    auto decoded = wire::decode_rpc_response(inner);
    if(!decoded)
        return false;

    auto peer_it = table.find(node_name);
    if(peer_it == table.end())
        return drop_orphan_response(f.m_logger);

    auto entry_it = peer_it->second.find(decoded->header.correlation_id);
    if(entry_it == peer_it->second.end())
        return drop_orphan_response(f.m_logger);

    typename Forwarder::pending_rpc pending = std::move(entry_it->second);
    peer_it->second.erase(entry_it);
    pending.timer->cancel();
    detail::emit_rpc_reply(f, pending.fqn, decoded->header.correlation_id, decoded->status);
    pending.on_response(decoded->status, decoded->return_data);
    return true;
}

}

#endif
