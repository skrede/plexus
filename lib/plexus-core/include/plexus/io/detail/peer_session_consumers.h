#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H

#include "plexus/io/frame_router.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_control.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_originate.h"

#include "plexus/io/security/attach_facts.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

template<typename Session>
void register_handshake_consumers(Session &s)
{
    s.m_router.on_handshake_req(
            [&s](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_handshake_request(inner))
                {
                    s.m_negotiator.assemble(*req, security::attach_role::responder, req->id, s.m_fsm_cfg.self_id, s.m_fsm.attach_policy() != nullptr);
                    s.m_negotiator.latch_peer_proof(*req);
                    if(s.m_negotiator.posture_mismatched(*req))
                        return refuse_posture(s);
                    execute(s, s.m_fsm.on_request(*req, s.m_is_inbound_bootstrap, s.m_negotiator.facts()));
                }
            });
    s.m_router.on_handshake_resp(
            [&s](std::span<const std::byte> inner)
            {
                if(auto resp = wire::decode_handshake_response(inner))
                {
                    s.m_negotiator.assemble(*resp, security::attach_role::initiator, resp->id, s.m_fsm_cfg.self_id, s.m_fsm.attach_policy() != nullptr);
                    s.m_negotiator.latch_peer_proof(*resp);
                    if(s.m_negotiator.posture_mismatched(*resp))
                        return refuse_posture(s);
                    execute(s, s.m_fsm.on_response(*resp, s.m_negotiator.facts()));
                }
            });
}

template<typename Session, typename Procedures>
void register_data_consumers(Session &s, frame_router &router, Procedures &procedures)
{
    // The plain unidirectional receive: deliver locally, then (relay-only) re-originate as a forwarded
    // envelope so a directly-attached publisher's publish transits onward to remote subscribers.
    router.on_unidirectional(
            [&s](const wire::frame_header &hdr, std::span<const std::byte> inner)
            {
                deliver_session_data(s, hdr, inner);
                originate_if_pubsub(s, hdr, inner);
            });
    router.on_rpc_request([&s, &procedures](std::span<const std::byte> inner) { procedures.deliver_request(s.rpc_peer(), inner, s.session_id()); });
    router.on_rpc_response([&s, &procedures](std::span<const std::byte> inner) { procedures.deliver_response(s.rpc_peer(), inner); });
}

template<typename Session>
void register_session_consumers(Session &s)
{
    register_handshake_consumers(s);
    register_control_consumers(s, s.m_router, s.m_messages);
    register_data_consumers(s, s.m_router, s.m_procedures);
    // A heartbeat stamps this pinned peer's last-seen only — presence, never a topic deadline.
    s.m_router.on_heartbeat(
            [&s](std::span<const std::byte> inner)
            {
                const auto hb = wire::decode_heartbeat(inner);
                if(!hb)
                    return;
                if(s.m_on_stamp_seen_cb)
                    s.m_on_stamp_seen_cb(s.m_ctx.peer_id);
                // The relay honor points key on the handshake-proven identity (m_reported, the broadcast
                // self-skip), not the provisional context id a dialed/accepted slot carries — so the
                // decline bit is consumed only once the handshake has settled who is speaking, matching
                // the peer_report and forwarded receive halves. A pre-handshake heartbeat stamps
                // presence but must not reach the relay's decline honor state.
                if(s.is_complete() && s.m_on_decline_seen_cb)
                    s.m_on_decline_seen_cb(s.peer_identity(), (hb->reserved & wire::k_heartbeat_relay_decline_flag) != 0);
            });
}

}

#endif
